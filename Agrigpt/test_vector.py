# test_vector.py
from rag.retriever_vector import search

def main():
    query = "টমেটো চাষের তাপমাত্রা ও আর্দ্রতা"
    print(f"🔎 Query: {query}")

    try:
        hits = search(query, k=5, fetch_k=32, language="bn")
    except Exception as e:
        print("💥 ERROR: Could not run search:", e)
        return

    print(f"✅ {len(hits)} hits returned\n")

    for i, h in enumerate(hits[:5], 1):
        text_snip = h.get("text", "").replace("\n", " ")
        if len(text_snip) > 120:
            text_snip = text_snip[:120] + "..."
        print(f"{i}. {h.get('source','?')} → {text_snip}")

if __name__ == "__main__":
    main()
