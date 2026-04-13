import os
import time
import threading
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

MODEL_NAME = os.getenv("NLLB_MODEL", "facebook/nllb-200-distilled-600M")
MODEL_LOCAL_DIR = os.getenv("NLLB_LOCAL_DIR", "/app/models/nllb")
BACKGROUND_LOAD = os.getenv("NLLB_BACKGROUND_LOAD", "1") == "1"
BLOCK_ON_FIRST_REQUEST = os.getenv("BLOCK_ON_FIRST_REQUEST", "0") == "1"
DEVICE = "cpu"  # CPU-only; set LOCAL_GPU_AVAILABLE=1 to re-enable GPU when available

app = FastAPI(
    title="NLLB Translator",
    version="1.3.1",
    description="Translator microservice using deferred background model loading."
)

_lock = threading.Lock()
_ready = False
_model = None
_tokenizer = None
_lang_code_to_id = None
_load_started = None
_load_finished = None
_last_error: Optional[str] = None

def _do_load():
    global _ready, _model, _tokenizer, _lang_code_to_id, _load_started, _load_finished, _last_error
    with _lock:
        if _ready:
            return
        _last_error = None
        _load_started = time.time()
        print(f"[serve_nllb] os.path.isdir({MODEL_LOCAL_DIR}) = {os.path.isdir(MODEL_LOCAL_DIR)}")
        try:
            print(f"[serve_nllb] Directory listing for {MODEL_LOCAL_DIR}: {os.listdir(MODEL_LOCAL_DIR)}")
        except Exception as e:
            print(f"[serve_nllb] Could not list {MODEL_LOCAL_DIR}: {e}")
        base_path = MODEL_LOCAL_DIR if os.path.isdir(MODEL_LOCAL_DIR) else MODEL_NAME
        print(f"[serve_nllb] Loading tokenizer from {base_path} ...")
        try:
            _tokenizer = AutoTokenizer.from_pretrained(base_path, use_fast=False)
            print(f"[serve_nllb] Tokenizer loaded.")
            # --- PATCH: Support both lang_code_to_id and lang_token_to_id ---
            _lang_code_to_id = getattr(_tokenizer, "lang_code_to_id", None)
            if _lang_code_to_id is None:
                _lang_code_to_id = getattr(_tokenizer, "lang_token_to_id", None)
            if _lang_code_to_id is None:
                raise RuntimeError("Tokenizer does not support language code mapping (lang_code_to_id or lang_token_to_id)")
            print(f"[serve_nllb] Loading model weights from {base_path} ...")
            _model = AutoModelForSeq2SeqLM.from_pretrained(base_path)
            print(f"[serve_nllb] Model weights loaded.")
            print(f"[serve_nllb] Moving model to {DEVICE} ...")
            _model = _model.to(DEVICE)
            print(f"[serve_nllb] Model moved to {DEVICE}.")
            print(f"[serve_nllb] Setting model to eval mode ...")
            _model.eval()
            print(f"[serve_nllb] Model set to eval mode.")
            _load_finished = time.time()
            _ready = True
            print(f"[serve_nllb] Model loaded from {base_path} in {round(_load_finished - _load_started,2)}s on {DEVICE}")
        except Exception as e:
            _last_error = str(e)
            print(f"[serve_nllb] ERROR during model load: {e}")

def ensure_loaded(background: bool):
    if _ready:
        return
    if background:
        if not any(t.name == "nllb-loader" for t in threading.enumerate()):
            threading.Thread(target=_do_load, name="nllb-loader", daemon=True).start()
    else:
        _do_load()

@app.on_event("startup")
def startup():
    ensure_loaded(background=BACKGROUND_LOAD)

class TranslateRequest(BaseModel):
    text: str = Field(..., min_length=1)
    src: str = Field("fra_Latn")
    tgt: str = Field("eng_Latn")
    max_length: int = Field(200, ge=4, le=512)

class TranslateResponse(BaseModel):
    translation: str
    src: str
    tgt: str
    model: str
    device: str
    generation_time_ms: float
    loaded_seconds: Optional[float]

@app.get("/healthz")
def healthz():
    return {"ok": True, "ready": _ready, "error": _last_error}

@app.get("/")
def root():
    return {
        "status": "ok",
        "model": MODEL_NAME,
        "device": DEVICE,
        "ready": _ready,
        "error": _last_error,
        "load_seconds": None if not _ready or not _load_finished else round(_load_finished - _load_started, 2),
        "cached_dir_exists": os.path.isdir(MODEL_LOCAL_DIR),
    }

@app.post("/translate", response_model=TranslateResponse)
def translate(req: TranslateRequest):
    if not _ready:
        if BLOCK_ON_FIRST_REQUEST:
            ensure_loaded(background=False)
        else:
            raise HTTPException(status_code=503, detail="Model still loading")
    if _last_error:
        raise HTTPException(status_code=500, detail=f"Model load error: {_last_error}")

    if req.src not in _lang_code_to_id:
        raise HTTPException(status_code=400, detail=f"Unsupported src language code: {req.src}")
    if req.tgt not in _lang_code_to_id:
        raise HTTPException(status_code=400, detail=f"Unsupported tgt language code: {req.tgt}")

    try:
        with torch.inference_mode():
            inputs = _tokenizer(req.text, return_tensors="pt", src_lang=req.src).to(DEVICE)
            start = time.time()
            generated = _model.generate(
                **inputs,
                forced_bos_token_id=_lang_code_to_id[req.tgt],
                max_length=req.max_length
            )
            out = _tokenizer.decode(generated[0], skip_special_tokens=True)
            duration_ms = (time.time() - start) * 1000.0
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Translation failed: {e}")

    load_time = None
    if _ready and _load_started and _load_finished:
        load_time = round(_load_finished - _load_started, 2)

    return TranslateResponse(
        translation=out,
        src=req.src,
        tgt=req.tgt,
        model=MODEL_NAME,
        device=DEVICE,
        generation_time_ms=round(duration_ms, 2),
        loaded_seconds=load_time
    )

if __name__ == "__main__":
    # Enforce using the Cloud Run port (must be exported) fallback 8080
    port = int(os.environ.get("PORT") or "8080")
    print(f"[serve_nllb] Starting uvicorn on port {port} (ready={_ready})")
    ensure_loaded(background=BACKGROUND_LOAD)
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)