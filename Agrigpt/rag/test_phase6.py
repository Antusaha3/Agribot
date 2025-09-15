# rag/test_phase6.py
from __future__ import annotations
import time
from .router import answer

TESTS = [
    # ---------- GraphRAG: Season/Climate (Crop-level) ----------
    "আমন ধানের মৌসুম ও তাপমাত্রা কত?",
    "আউশ ধানের মৌসুম কী?",
    "বোরো ধানের তাপমাত্রা কত ডিগ্রি সহনীয়?",
    "Wheat (গম) এর মৌসুম কী?",
    "Aman rice season?",
    "Boro rice temperature range?",
    "aus ধানের মৌসুম?",
    "সহজ ভাষায় আমন ধানের মৌসুম বলো",
    "বোরো ধান কোন মৌসুমে চাষ হয়?",
    "Aman এর humidity range কত?",

    # ---------- GraphRAG: District-wise timing from SPAS (if present) ----------
    "দিনাজপুরে আমন ধানের রোপণের সময় কখন?",
    "ময়মনসিংহে আমন ধান কাটাই কবে?",
    "রাজশাহীতে গম রোপণ/কাটা সময় কী?",
    "বগুড়ায় আমন ধানের মৌসুম কী?",
    "Rangpur এ Aman transplant window?",

    # ---------- Aliases / Mixed language / Slug tests ----------
    "Aman ধান মৌসুম?",
    "Kharif 2 season crop Aman?",
    "খরিফ-১ কোন ধানের সাথে সম্পর্কিত?",
    "রবি মৌসুমে কোন ধান চাষ হয়?",

    # ---------- Disease queries (will be 'মাফ করবেন…' until you load diseases + router disease branch) ----------
    "ধানের রোগ কী কী?",
    "বোরো ধানের প্রধান রোগ কী?",
    "গমের রোগ কী?",
    "Tomato diseases list?",

    # ---------- Fertilizer (optional; expect fallback/sorry unless you load fert edges) ----------
    "চালের ফলন বাড়াতে কোন সার দরকার?",
    "গমে NPK ডোজ কত হওয়া উচিত?",

    # ---------- Other crops (add to KG later; good for refusal/fallback) ----------
    "কুমড়া চাষের উপযুক্ত জলবায়ু কেমন?",
    "টমেটো চাষের তাপমাত্রা ও আর্দ্রতা কত হওয়া উচিত?",
    "ভুট্টার মৌসুম কী?",
    "পেঁয়াজের মৌসুম কী?",

    # ---------- Unknown/edge ----------
    "অজানা ফসল X এর মৌসুম কী?",
    "ধানের মৌসুম এক লাইনে বলো",
]

def run_all(k: int = 5, max_chars: int = 1200):
    for i, q in enumerate(TESTS, 1):
        print(f"\n{i:02d} ❓ {q}")
        t0 = time.time()
        try:
            out = answer(q, k=k)
        except Exception as e:
            print(f"💥 ERROR: {e}")
            print("-" * 60)
            continue
        dt = time.time() - t0
        snip = (out or "").strip()
        if len(snip) > max_chars:
            snip = snip[:max_chars] + "…"
        print("✅ উত্তর:\n", snip)
        print(f"⏱️  {dt:.2f}s")
        print("-" * 60)

def main():
    run_all(k=5)

if __name__ == "__main__":
    main()
