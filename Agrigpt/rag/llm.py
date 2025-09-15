# rag/llm.py
from __future__ import annotations
import os
from typing import Optional

_PIPE = None
_ERR  = None

def _lazy_load():
    global _PIPE, _ERR
    if _PIPE is not None or _ERR is not None:
        return
    try:
        from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline
        model_name = os.getenv("HUGGINGFACE_MODEL", "").strip()
        if not model_name:
            _ERR = "HUGGINGFACE_MODEL not set"
            return
        tok = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        model = AutoModelForCausalLM.from_pretrained(
            model_name,
            device_map="auto",
            torch_dtype="auto",
            trust_remote_code=True,
        )
        _PIPE = pipeline(
            "text-generation",
            model=model,
            tokenizer=tok,
            batch_size=1
        )
    except Exception as e:
        _ERR = f"load_error: {e}"

def generate(text: str) -> Optional[str]:
    """
    Returns LLM output string or None (if model not available).
    """
    _lazy_load()
    if _ERR or _PIPE is None:
        return None
    max_new = int(os.getenv("HUGGINGFACE_MAX_NEW_TOKENS", "280"))
    temperature = float(os.getenv("HUGGINGFACE_TEMPERATURE", "0.2"))
    out = _PIPE(
        text,
        max_new_tokens=max_new,
        do_sample=(temperature > 0),
        temperature=temperature,
        pad_token_id=_PIPE.tokenizer.eos_token_id,
    )
    # transformers returns a list of dicts
    return out[0]["generated_text"]
