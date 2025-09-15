# tools/make_aliases.py
import csv, re, os
from pathlib import Path

# === INPUT / OUTPUT PATHS (adjust if needed) ===
NODES_CROPS = Path("graph/csv/nodes_crops.csv")
ALIASES_OUT = Path("graph/csv/aliases.csv")

def norm(s):
    return (s or "").strip().lower()

def contains(hay, needle):
    return needle in norm(hay)

def any_contains(row, *needles):
    name_bn = row.get("name_bn") or row.get("c.name_bn") or ""
    name_en = row.get("name_en") or row.get("c.name_en") or ""
    cid     = row.get("id")      or row.get("c.id")      or ""
    text = " ".join([name_bn, name_en, cid]).lower()
    return any(n in text for n in needles)

# Bangla alias -> list of English/ID keywords we’ll try to match in nodes_crops
ALIAS_RULES = {
    # paddy umbrella & season groups
    "ধান":      [["aman"], ["boro"], ["aus", "aus-rice"]],
    "আমন":     [["aman"]],
    "বোরো":    [["boro"]],
    "আউশ":     [["aus", "aus-rice"]],
    # staples, pulses, oilseeds, cash crops
    "গম":       [["wheat"]],
    "ভুট্টা":   [["maize"], ["corn"]],
    "জুট":      [["jute"]],
    "পাট":      [["jute"]],
    "আলু":      [["potato"]],
    "সরিষা":    [["mustard"]],
    "তিল":      [["sesame"]],
    "চিনাবাদাম":[["groundnut"], ["peanut"]],
    "সয়াবিন":  [["soybean"], ["soya"]],
    "সয়াবিন":  [["soybean"], ["soya"]],
    "মসুর":     [["lentil"], ["masur"]],
    "মুগ":      [["mung"], ["mungbean"], ["moog"]],
    "মুগডাল":   [["mung"], ["mungbean"], ["moog"]],
    "ছোলা":     [["chickpea"]],
    "খেসারি":   [["grass-pea"], ["khesari"]],
    "মটর":      [["pea"]],
    # high-freq vegetables
    "পেঁয়াজ":  [["onion"]],
    "রসুন":     [["garlic"]],
    "মরিচ":     [["chili"], ["chilli"], ["capsicum"]],
    "টমেটো":    [["tomato"]],
    "বেগুন":    [["eggplant"], ["brinjal"]],
    "ঢেঁড়স":   [["okra"], ["ladys finger"], ["lady's finger"]],
    "শসা":      [["cucumber"]],
    "কুমড়া":   [["pumpkin"]],
    "লাউ":      [["bottle gourd"]],
    "করলা":     [["bitter gourd"]],
    "ঝিঙে":    [["ridge gourd"]],
    "চিচিঙ্গা": [["snake gourd"]],
}

def guess_matches(row, keyword_list):
    """Return True if any of the keyword variants appear in id/name."""
    name_bn = row.get("name_bn") or row.get("c.name_bn") or ""
    name_en = row.get("name_en") or row.get("c.name_en") or ""
    cid     = row.get("id")      or row.get("c.id")      or ""
    text = " ".join([name_bn, name_en, cid]).lower()
    return any(all(k in text for k in kw) for kw in keyword_list)

def build_alias_rows(crop_rows):
    out = set()  # (alias, crop_id)
    for row in crop_rows:
        crop_id = row.get("id") or row.get("c.id")
        if not crop_id:
            continue

        # handle rice umbrella tokens via id/name pattern
        if any_contains(row, "aman"):
            out.add(("আমন", crop_id))
            out.add(("ধান", crop_id))
        if any_contains(row, "boro"):
            out.add(("বোরো", crop_id))
            out.add(("ধান", crop_id))
        # common Aus patterns
        if any_contains(row, "aus", "aus-rice"):
            out.add(("আউশ", crop_id))
            out.add(("ধান", crop_id))

        # generic mapping table
        for alias_bn, keyword_list in ALIAS_RULES.items():
            if guess_matches(row, keyword_list):
                out.add((alias_bn, crop_id))

    return sorted(out)

def main():
    if not NODES_CROPS.exists():
        raise SystemExit(f"Not found: {NODES_CROPS.resolve()}")

    # read crops
    with NODES_CROPS.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    alias_rows = build_alias_rows(rows)

    # write aliases.csv
    ALIASES_OUT.parent.mkdir(parents=True, exist_ok=True)
    with ALIASES_OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["alias", "crop_id"])
        w.writerows(alias_rows)

    print(f"✅ Wrote {len(alias_rows)} alias links -> {ALIASES_OUT}")

if __name__ == "__main__":
    main()
