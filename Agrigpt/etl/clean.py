# etl/clean.py
from __future__ import annotations
import json, re, unicodedata, hashlib
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

# ----------------------------
# Paths
# ----------------------------
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "D:/project=2/Agrigpt/data_csv/raw"
INTERIM = ROOT / "D:/project=2/Agrigpt/notebook/data/interim"
REPORTS = ROOT / "reports"
INTERIM.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Helpers: normalization & utils
# ----------------------------
_ZW = "".join(["\u200b", "\u200c", "\u200d", "\ufeff"])  # ZWSP, ZWNJ, ZWJ, BOM
_ZW_RE = re.compile(f"[{re.escape(_ZW)}]")

def normalize_bn(text: str) -> str:
    if text is None:
        return ""
    # Normalize Unicode (NFC), remove zero-width chars, normalize spaces
    t = unicodedata.normalize("NFC", str(text))
    t = _ZW_RE.sub("", t)
    t = t.replace("\xa0", " ")                 # no-break space
    t = re.sub(r"\s+", " ", t).strip()
    return t

def to_int(x):
    try: return int(x)
    except: return None

def to_float(x):
    try: return float(x)
    except: return None

def tokens(text: str) -> int:
    # simple whitespace token count (no external tokenizer)
    return len((text or "").split())

def sha1_12(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]

# ----------------------------
# Metadata record
# ----------------------------
@dataclass
class LineMeta:
    source: str
    doc_id: str
    line_id: str
    section_path: str
    language: str
    title: str
    created_at: str
    fields: Dict

def write_lines_and_meta(
    dst_txt: Path,
    dst_meta_jsonl: Path,
    lines_with_meta: Iterable[Tuple[str, LineMeta]],
):
    n = 0
    with dst_txt.open("w", encoding="utf-8") as ftxt, dst_meta_jsonl.open("w", encoding="utf-8") as fmeta:
        for txt, meta in lines_with_meta:
            ftxt.write(txt + "\n")
            fmeta.write(json.dumps(asdict(meta), ensure_ascii=False) + "\n")
            n += 1
    return n

# ----------------------------
# 1) Bangladesh Agricultural Dataset → prose
# ----------------------------
def preprocess_bangladesh_agri(limit: Optional[int] = None) -> Tuple[int, int]:
    # pick the one CSV in raw folder
    srcs = sorted((RAW / "bangladesh_agri").glob("*.csv"))
    if not srcs:
        print("[bangladesh_agri] No CSV found.")
        return 0, 0
    src = srcs[0]
    df = pd.read_csv(src)
    before_rows = len(df)

    # rename columns to stable keys
    df = df.rename(columns={
        "Products name": "crop_name",
        "Crops Type": "crop_type",
        "Max Temp": "max_temp_c",
        "Min Temp": "min_temp_c",
        "Max Relative Humidity": "max_rh",
        "Min Relative Humidity": "min_rh",
        "Season": "season",
        "Transplant": "transplant",
        "Growth": "growth",
        "Harvest": "harvest",
        "Country": "country",
    })

    # normalize fields
    for col in ["crop_name", "crop_type", "season", "transplant", "growth", "harvest", "country"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_bn)

    for col in ["max_temp_c", "min_temp_c", "max_rh", "min_rh"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if limit:
        df = df.head(limit)

    # convert each row to a Bangla-friendly sentence (mixed ok)
    lines = []
    created_at = datetime.utcnow().isoformat() + "Z"
    title = "Bangladesh Agricultural Dataset (104 crops)"
    for idx, r in df.iterrows():
        crop = r.get("crop_name", "")
        season = r.get("season", "")
        trans = r.get("transplant", "")
        growth = r.get("growth", "")
        harvest = r.get("harvest", "")
        tmin = r.get("min_temp_c", None)
        tmax = r.get("max_temp_c", None)
        rhmin = r.get("min_rh", None)
        rhmax = r.get("max_rh", None)
        ctry = r.get("country", "")

        temp_txt = ""
        if pd.notna(tmin) or pd.notna(tmax):
            if pd.notna(tmin) and pd.notna(tmax):
                temp_txt = f"তাপমাত্রা: {tmin}–{tmax}°C"
            elif pd.notna(tmin):
                temp_txt = f"তাপমাত্রা: ≥{tmin}°C"
            else:
                temp_txt = f"তাপমাত্রা: ≤{tmax}°C"

        rh_txt = ""
        if pd.notna(rhmin) or pd.notna(rhmax):
            if pd.notna(rhmin) and pd.notna(rhmax):
                rh_txt = f"আপেক্ষিক আর্দ্রতা: {rhmin}–{rhmax}%"
            elif pd.notna(rhmin):
                rh_txt = f"আপেক্ষিক আর্দ্রতা: ≥{rhmin}%"
            else:
                rh_txt = f"আপেক্ষিক আর্দ্রতা: ≤{rhmax}%"

        # Final prose line (Bangla bullets compressed into one sentence)
        parts = [
            f"ফসল: {crop}" if crop else None,
            f"মৌসুম: {season}" if season else None,
            f"রোপণ: {trans}" if trans else None,
            f"বৃদ্ধি: {growth}" if growth else None,
            f"কাটাই: {harvest}" if harvest else None,
            temp_txt or None,
            rh_txt or None,
            f"দেশ: {ctry}" if ctry else None,
        ]
        text = " | ".join([p for p in parts if p])

        doc_id = f"doc_{sha1_12(title)}"
        line_id = f"chunk_{doc_id}_{idx:04d}"
        section_path = f"{crop or 'Unknown crop'} > Season > {season or 'Unknown'}"
        meta = LineMeta(
            source="bangladesh_agri",
            doc_id=doc_id,
            line_id=line_id,
            section_path=section_path,
            language="bn",  # mixed bn/en ok
            title=title,
            created_at=created_at,
            fields={
                "crop_name": crop,
                "season": season,
                "transplant": trans,
                "growth": growth,
                "harvest": harvest,
                "min_temp_c": to_float(tmin),
                "max_temp_c": to_float(tmax),
                "min_rh": to_int(rhmin),
                "max_rh": to_int(rhmax),
                "country": ctry,
            },
        )
        lines.append((text, meta))

    out_txt = INTERIM / "bangladesh_agri_clean.txt"
    out_meta = INTERIM / "bangladesh_agri_clean.jsonl"
    n = write_lines_and_meta(out_txt, out_meta, lines)
    return before_rows, n

# ----------------------------
# 2) SPAS Dataset → prose
# ----------------------------
def preprocess_spas(limit: Optional[int] = None) -> Tuple[int, int]:
    srcs = sorted((RAW / "spas_bd").glob("SPAS-Dataset-BD*.csv"))
    if not srcs:
        print("[spas_bd] No CSV found.")
        return 0, 0
    src = srcs[0]
    df = pd.read_csv(src)
    before_rows = len(df)

    df = df.rename(columns={
        "Area": "area",
        "AP Ratio": "ap_ratio",
        "District": "district",
        "Season": "season",
        "Avg Temp": "avg_temp_c",
        "Avg Humidity": "avg_humidity",
        "Crop Name": "crop_name",
        "Transplant": "transplant",
        "Growth": "growth",
        "Harvest": "harvest",
        "Production": "production",
        "Max Temp": "max_temp_c",
        "Min Temp": "min_temp_c",
        "Max Relative Humidity": "max_rh",
        "Min Relative Humidity": "min_rh",
    })

    # Normalize strings
    for col in ["district", "season", "crop_name", "transplant", "growth", "harvest", "ap_ratio"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_bn)

    # Numerics
    for col in ["avg_temp_c", "avg_humidity", "production", "max_temp_c", "min_temp_c", "max_rh", "min_rh", "area"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if limit:
        df = df.head(limit)

    lines = []
    created_at = datetime.utcnow().isoformat() + "Z"
    title = "SPAS-Dataset-BD (Agronomy + Climate)"
    for idx, r in df.iterrows():
        crop = r.get("crop_name", "")
        dist = r.get("district", "")
        season = r.get("season", "")
        trans = r.get("transplant", "")
        growth = r.get("growth", "")
        harvest = r.get("harvest", "")
        avg_t = r.get("avg_temp_c", None)
        max_t = r.get("max_temp_c", None)
        min_t = r.get("min_temp_c", None)
        avg_h = r.get("avg_humidity", None)
        max_rh = r.get("max_rh", None)
        min_rh = r.get("min_rh", None)
        prod = r.get("production", None)

        parts = [
            f"ফসল: {crop}" if crop else None,
            f"জেলা: {dist}" if dist else None,
            f"মৌসুম: {season}" if season else None,
            f"রোপণ: {trans}" if trans else None,
            f"বৃদ্ধি: {growth}" if growth else None,
            f"কাটাই: {harvest}" if harvest else None,
            f"গড় তাপমাত্রা: {avg_t}°C" if pd.notna(avg_t) else None,
            f"তাপমাত্রা: {min_t}–{max_t}°C" if pd.notna(min_t) and pd.notna(max_t) else None,
            f"গড় আর্দ্রতা: {avg_h}%" if pd.notna(avg_h) else None,
            f"আপেক্ষিক আর্দ্রতা: {min_rh}–{max_rh}%" if pd.notna(min_rh) and pd.notna(max_rh) else None,
            f"উৎপাদন: {int(prod)}" if pd.notna(prod) else None,
        ]
        text = " | ".join([p for p in parts if p])

        doc_id = f"doc_{sha1_12(title)}"
        line_id = f"chunk_{doc_id}_{idx:04d}"
        section_path = f"{crop or 'Unknown crop'} > District > {dist or 'Unknown'} > Season > {season or 'Unknown'}"
        meta = LineMeta(
            source="spas_bd",
            doc_id=doc_id,
            line_id=line_id,
            section_path=section_path,
            language="bn",
            title=title,
            created_at=created_at,
            fields={
                "crop_name": crop,
                "district": dist,
                "season": season,
                "transplant": trans,
                "growth": growth,
                "harvest": harvest,
                "avg_temp_c": to_float(avg_t),
                "max_temp_c": to_float(max_t),
                "min_temp_c": to_float(min_t),
                "avg_humidity": to_float(avg_h),
                "min_rh": to_int(min_rh),
                "max_rh": to_int(max_rh),
                "production": to_int(prod),
            },
        )
        lines.append((text, meta))

    out_txt = INTERIM / "spas_bd_clean.txt"
    out_meta = INTERIM / "spas_bd_clean.jsonl"
    n = write_lines_and_meta(out_txt, out_meta, lines)
    return before_rows, n

# ----------------------------
# 3) UDDIPOK → cleaned eval CSV (not included in retrieval corpus)
# ----------------------------
def preprocess_uddipok(limit: Optional[int] = None) -> Tuple[int, int]:
    srcs = sorted((RAW / "uddipok").glob("RC_Dataset_v2*.csv"))
    if not srcs:
        print("[uddipok] No CSV found.")
        return 0, 0
    src = srcs[0]
    df = pd.read_csv(src)
    before_rows = len(df)

    # drop unnamed index columns, strip and drop null Questions
    drop_cols = [c for c in df.columns if c.lower().startswith("unnamed")]
    df = df.drop(columns=drop_cols, errors="ignore")
    for c in ["Passage", "Question", "AnsText"]:
        if c in df.columns:
            df[c] = df[c].apply(normalize_bn)
    df = df[df["Question"].notna() & (df["Question"].str.len() > 0)]

    if limit:
        df = df.head(limit)

    out_csv = INTERIM / "uddipok_eval_clean.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8")
    return before_rows, len(df)

# ----------------------------
# Reporting
# ----------------------------
def summarize_file(path_txt: Path) -> Dict:
    if not path_txt.exists():
        return {"exists": False}
    lines = [l.strip() for l in path_txt.read_text(encoding="utf-8").splitlines() if l.strip()]
    lengths = [tokens(l) for l in lines]
    return {
        "exists": True,
        "lines": len(lines),
        "avg_tokens": round(sum(lengths)/len(lengths), 2) if lengths else 0.0,
        "p95_tokens": sorted(lengths)[int(len(lengths)*0.95)-1] if lengths else 0,
        "max_tokens": max(lengths) if lengths else 0,
    }

def main(limit: Optional[int] = None):
    print("== Preprocess Bangladesh Agri ==")
    ba_before, ba_after = preprocess_bangladesh_agri(limit=limit)
    print(f"Rows before: {ba_before} -> lines after: {ba_after}")

    print("== Preprocess SPAS ==")
    sp_before, sp_after = preprocess_spas(limit=limit)
    print(f"Rows before: {sp_before} -> lines after: {sp_after}")

    print("== Clean UDDIPOK (eval only) ==")
    ud_before, ud_after = preprocess_uddipok(limit=limit)
    print(f"Rows before: {ud_before} -> rows after: {ud_after}")

    # quality report
    report = pd.DataFrame([
        {"file": "bangladesh_agri_clean.txt", **summarize_file(INTERIM / "bangladesh_agri_clean.txt")},
        {"file": "spas_bd_clean.txt", **summarize_file(INTERIM / "spas_bd_clean.txt")},
    ])
    out = REPORTS / "preprocess_quality_report.csv"
    report.to_csv(out, index=False)
    print("Wrote report →", out)
    print(report)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Clean & normalize raw sources into data/interim/ (idempotent).")
    p.add_argument("--limit", type=int, default=None, help="Optional: process only the first N rows for quick tests")
    args = p.parse_args()
    main(limit=args.limit)

