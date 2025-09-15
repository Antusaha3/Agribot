# ui/app.py
from __future__ import annotations
import os
import time
import requests
import streamlit as st

API_URL = os.getenv("AGRIGPT_API_URL", "http://127.0.0.1:8000")

st.set_page_config(
    page_title="AgriGPT – বাংলাদেশের কৃষি সহকারী",
    page_icon="🌾",
    layout="centered",
)

# -------------------- minimal styles --------------------
st.markdown("""
<style>
:root {
  --agri-green: #1c7c3f;
  --agri-dark:  #0f172a;
}
html, body, [class*="css"] {
  font-family: "Noto Sans Bengali", "Hind Siliguri", system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, "Noto Sans", "Apple Color Emoji", "Segoe UI Emoji";
}
header {visibility: hidden;}
.block-container {padding-top: 1.2rem; max-width: 900px;}
.agri-hero {
  background: var(--agri-green);
  color: #fff; border-radius: 16px;
  padding: 14px 18px; margin-bottom: 8px;
}
.agri-hero h1 {margin: 0; font-size: 28px;}
.agri-hero p  {margin: 2px 0 0; opacity: .95;}
.badge {
  display:inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px;
  background:#0ea5e9; color:#fff; margin-left: 8px;
}
.badge.graph { background:#16a34a; }
.badge.vector{ background:#0ea5e9; }
.answer-card {
  border: 1px solid #234;
  border-radius: 12px;
  padding: 14px 16px;
  background: rgba(28,124,63,0.06);
}
.source-box {
  border-left: 3px solid var(--agri-green);
  padding-left: 10px;
  margin-top: 8px;
}
.quickchips button {
  margin-right: 6px; margin-bottom: 6px;
}
small.muted { opacity: .8; }
</style>
""", unsafe_allow_html=True)

# -------------------- header --------------------
st.markdown("""
<div class="agri-hero">
  <h1>AgriGPT — বাংলাদেশের কৃষি সহকারী</h1>
  <p>ফসলের মৌসুম, জলবায়ু, রোগ-পোকা ও সার সংক্রান্ত প্রশ্নের সংক্ষিপ্ত উত্তর — বাংলায়।</p>
</div>
""", unsafe_allow_html=True)

# -------------------- quick chips --------------------
DEFAULT_QUESTIONS = [
    "আমন ধানের মৌসুম ও তাপমাত্রা কত?",
    "আউশ ধানের মৌসুম কী?",
    "বোরো ধানের তাপমাত্রা কত ডিগ্রি সহনীয়?",
    "ধানের রোগ কী কী?",
    "টমেটো চাষের তাপমাত্রা ও আর্দ্রতা কত?",
]

st.caption("🟢 ফ্রত প্রশ্ন")
cols = st.columns([1,1,1,1,1])
for i, q in enumerate(DEFAULT_QUESTIONS):
    if cols[i].button(q, use_container_width=True):
        st.session_state["query"] = q

# -------------------- main form --------------------
with st.form("ask"):
    q = st.text_input(
        "আপনার প্রশ্ন লিখুন",
        value=st.session_state.get("query", ""),
        placeholder="যেমন: টমেটো চাষের তাপমাত্রা ও আর্দ্রতা কত হওয়া উচিত?",
    )
    submitted = st.form_submit_button("উত্তর পান", use_container_width=True)

if submitted and q.strip():
    with st.spinner("উত্তর আনছি…"):
        t0 = time.time()
        try:
            resp = requests.post(
                f"{API_URL}/api/ask",
                json={"question": q.strip()},
                timeout=60,
            )
            if resp.status_code != 200:
                st.error(f"সার্ভার ত্রুটি: {resp.status_code} — {resp.text[:200]}")
            else:
                data = resp.json()
                mode = (data.get("mode") or "").lower()
                badge = '<span class="badge graph">Graph</span>' if mode == "graphrag" else '<span class="badge vector">Vector</span>'
                st.markdown(f"""<div class="answer-card">
                    <div><strong>উত্তর</strong> {badge}</div>
                    <div style="margin-top:8px">{data.get("answer","")}</div>
                </div>""", unsafe_allow_html=True)

                sources = data.get("sources") or []
                with st.expander("📚 উৎস (Sources)", expanded=False):
                    if not sources:
                        st.write("উৎস পাওয়া যায়নি।")
                    else:
                        for s in sources:
                            st.markdown(f"- {s}")
                st.caption(f"⏱️ সময় লেগেছে ~{time.time()-t0:.2f}s")
        except requests.exceptions.RequestException as e:
            st.error(f"সংযোগ পাওয়া যায়নি ({API_URL}) — {e}")
elif submitted:
    st.warning("প্রশ্ন লিখুন।")

# -------------------- footer --------------------
st.markdown("<small class='muted'>© AgriGPT — গবেশণা ও কৃষকের সেবা 🌾</small>", unsafe_allow_html=True)
