# rag/generator.py
from __future__ import annotations
import os
from dotenv import load_dotenv
from transformers import pipeline
import torch

load_dotenv()

# Defaults (can override via .env)
_MODEL_ID     = os.getenv("HUGGINGFACE_MODEL", "bigscience/bloomz-560m")
_HF_TOKEN     = os.getenv("HF_TOKEN")  # optional
_MAX_NEW      = int(os.getenv("HUGGINGFACE_MAX_NEW_TOKENS", 180))
_TEMPERATURE  = float(os.getenv("HUGGINGFACE_TEMPERATURE", 0.2))
_TOP_P        = float(os.getenv("HUGGINGFACE_TOP_P", 0.95))

_pipe = None

def _get_pipe():
    """
    Build and cache a text-generation pipeline.
    - Uses `token=...` (new HF arg) only if an HF token is provided.
    - Picks CUDA if available; else CPU (device=-1).
    - Sets dtype: float16 on CUDA, float32 on CPU.
    - Gracefully falls back to bloomz-560m if the requested model fails.
    """
    global _pipe
    if _pipe is not None:
        return _pipe

    has_cuda = torch.cuda.is_available()
    device = 0 if has_cuda else -1
    dtype = torch.float16 if has_cuda else torch.float32

    pipe_kwargs = {
        "model": _MODEL_ID,
        "tokenizer": _MODEL_ID,
        "device": device,
        "torch_dtype": dtype,
        "trust_remote_code": True,
    }
    if _HF_TOKEN:
        pipe_kwargs["token"] = _HF_TOKEN  # modern HF argument

    try:
        _pipe = pipeline("text-generation", **pipe_kwargs)
    except Exception as e:
        print(f"[generator] Failed to load {_MODEL_ID} → {e}")
        fallback = "bigscience/bloomz-560m"
        print(f"[generator] Falling back to: {fallback}")
        _pipe = pipeline(
            "text-generation",
            model=fallback,
            tokenizer=fallback,
            device=device,
            torch_dtype=dtype,
            trust_remote_code=True,
        )

    # Ensure we have a valid pad token id for generation
    try:
        tok = _pipe.tokenizer
        if getattr(tok, "pad_token_id", None) is None and getattr(tok, "eos_token_id", None) is not None:
            tok.pad_token_id = tok.eos_token_id
    except Exception:
        pass

    return _pipe

def _gen(prompt: str) -> str:
    pipe = _get_pipe()
    try:
        out = pipe(
            prompt,
            max_new_tokens=_MAX_NEW,
            do_sample=True,
            temperature=_TEMPERATURE,
            top_p=_TOP_P,
            # return only the continuation, not the whole prompt
            return_full_text=False,
            pad_token_id=getattr(pipe.tokenizer, "pad_token_id", None),
        )
        text = out[0]["generated_text"].strip()
        return text
    except Exception as e:
        return f"[generator error] {e}"

def gen_from_graph_facts(question: str, facts_block: str, sys_prompt: str) -> str:
    # Keep prompts simple; small models do better with one block
    prompt = sys_prompt + "\n\n" + facts_block
    return _gen(prompt)

def gen_from_passages(question: str, passages_block: str, sys_prompt: str) -> str:
    prompt = sys_prompt + "\n\n" + passages_block
    return _gen(prompt)
