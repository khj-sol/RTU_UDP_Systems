# -*- coding: utf-8 -*-
"""
Qwen3-VL WSL 추론 서버 — model_maker_web_v4

WSL(Linux)에서 실행. Windows 메인 프로세스가 HTTP로 호출.
gptqmodel/autoawq는 Linux에서만 정상 빌드 가능하므로 이 서버에서 처리.

포트: 8084  (QWEN_SERVER_PORT 환경변수로 오버라이드)
Endpoints:
  GET  /health  → {"loaded": bool, "model_path": str, "device": str, "error": str}
  POST /extract → {"image_b64": str, "prompt": str} → {"text": str}
"""
from __future__ import annotations

import base64
import io
import logging
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="QwenVL WSL Server")

# ── 설정 (환경변수로 오버라이드 가능) ─────────────────────────────────────────
MODEL_PATH = os.environ.get("QWEN_MODEL_PATH", "/mnt/c/models/Qwen3-VL-32B-4bit")
DEVICE = os.environ.get("QWEN_DEVICE", "auto")

# ── 싱글톤 ───────────────────────────────────────────────────────────────────
_model = None
_processor = None
_model_path_loaded: str = ""
_load_error: str = ""


def _load_model():
    global _model, _processor, _model_path_loaded, _load_error
    if _model is not None:
        return
    try:
        import transformers as _tf

        logger.info("[QwenVL-WSL] 모델 로드 중: %s (device=%s)", MODEL_PATH, DEVICE)

        # Qwen 버전별 클래스명 시도
        _model_cls = None
        for _cls_name in (
            "Qwen3VLForConditionalGeneration",
            "Qwen2_5_VLForConditionalGeneration",
            "Qwen2VLForConditionalGeneration",
        ):
            _cls = getattr(_tf, _cls_name, None)
            if _cls is not None:
                _model_cls = _cls
                logger.info("[QwenVL-WSL] 클래스: %s", _cls_name)
                break

        if _model_cls is None:
            raise ImportError(
                "Qwen VL 클래스를 찾을 수 없음 (transformers 버전 확인 필요)"
            )

        from transformers import AutoProcessor

        _processor = AutoProcessor.from_pretrained(
            MODEL_PATH, trust_remote_code=True
        )
        _model = _model_cls.from_pretrained(
            MODEL_PATH,
            device_map=DEVICE,
            dtype="auto",
            trust_remote_code=True,
        )
        _model.eval()
        _model_path_loaded = MODEL_PATH
        _load_error = ""
        logger.info("[QwenVL-WSL] 로드 완료")
    except Exception as e:
        _load_error = str(e)
        logger.error("[QwenVL-WSL] 로드 실패: %s", e)


@app.on_event("startup")
async def startup_event():
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _load_model)


# ── 요청/응답 모델 ─────────────────────────────────────────────────────────────
class ExtractRequest(BaseModel):
    image_b64: str       # base64 인코딩된 PNG bytes
    prompt: str
    max_new_tokens: int = 4096


# ── 엔드포인트 ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    if _model is not None:
        try:
            dev = str(next(_model.parameters()).device)
        except Exception:
            dev = "unknown"
        return {"loaded": True, "model_path": _model_path_loaded, "device": dev}
    return JSONResponse(
        status_code=503,
        content={"loaded": False, "error": _load_error or "모델 로드 중"},
    )


@app.post("/extract")
def extract(req: ExtractRequest):
    if _model is None:
        raise HTTPException(503, detail=f"모델 미로드: {_load_error}")

    import torch
    from PIL import Image

    img_bytes = base64.b64decode(req.image_b64)
    image = Image.open(io.BytesIO(img_bytes)).convert("RGB")

    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image", "image": image},
                {"type": "text", "text": req.prompt},
            ],
        }
    ]

    text_input = _processor.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )
    inputs = _processor(
        text=[text_input],
        images=[image],
        return_tensors="pt",
    ).to(_model.device)

    with torch.no_grad():
        output_ids = _model.generate(
            **inputs,
            max_new_tokens=req.max_new_tokens,
            do_sample=False,
            temperature=1.0,
        )

    input_len = inputs["input_ids"].shape[1] if "input_ids" in inputs else 0
    gen_ids = output_ids[0][input_len:]
    result = _processor.tokenizer.decode(gen_ids, skip_special_tokens=True).strip()

    return {"text": result}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("QWEN_SERVER_PORT", "8084"))
    uvicorn.run(app, host="0.0.0.0", port=port)
