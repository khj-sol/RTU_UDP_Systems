# -*- coding: utf-8 -*-
"""
Nemotron OCR v2 adapter -- V4 benchmark candidate

역할:
  1. extract_table_from_image() : 단일 이미지 -> 레지스터 JSON 배열
  2. extract_pages()            : 전체 페이지 순차 처리 + 주소 기준 중복 제거

모드:
  - API 클라이언트 모드: NVIDIA NIM (OpenAI-compatible /v1/chat/completions)
  - 로컬 HuggingFace 모드: 폴백 (VL 모델 직접 로드)

언어 변형:
  - lang='en'    : 영문 프롬프트 (nemotron_ocr_en)
  - lang='multi' : 한국어/일본어 레이블 처리 포함 (nemotron_ocr_multi)

호출처:
  - stage1.py  _run_nemotron_ocr_extraction()
"""
from __future__ import annotations

import base64
import io
import json
import logging
import os
import re
from typing import Any, TypedDict


class OcrRegion(TypedDict):
    """단일 페이지에서 추출된 OCR 텍스트 영역."""
    text: str          # 모델 원문 응답 텍스트
    confidence: float  # 모델이 confidence를 반환하면 0.0-1.0, 없으면 None
    bbox: list         # [x0, y0, x1, y1] 정규화 0-1, 없으면 None
    page: int          # 0-indexed 페이지 번호
    source: str        # 'nim_api' | 'local' | 'fixture_png'

logger = logging.getLogger(__name__)

# ── 싱글톤 ────────────────────────────────────────────────────────────────────
_nemotron_instance: "NemotronOCRModel | None" = None
_nemotron_error: str = ""


def get_nemotron_model(
    model_path: str = "",
    device: str = "auto",
    api_url: str = "",
    api_key: str = "",
    model_id: str = "nvidia/llama-3.1-nemotron-nano-vl-8b-v1",
) -> "NemotronOCRModel | None":
    """NemotronOCRModel 싱글톤. 이미 로드됐으면 재사용.

    api_url + api_key 있으면 NIM API 클라이언트 모드.
    없으면 로컬 HuggingFace 로드 모드.
    """
    global _nemotron_instance, _nemotron_error
    if _nemotron_instance is not None:
        return _nemotron_instance
    try:
        _nemotron_instance = NemotronOCRModel(
            model_path=model_path,
            device=device,
            api_url=api_url,
            api_key=api_key,
            model_id=model_id,
        )
        _nemotron_error = ""
        return _nemotron_instance
    except Exception as e:
        _nemotron_error = str(e)
        logger.warning("[NemotronOCR] 로드 실패: %s", e)
        return None


def get_nemotron_status() -> dict:
    """현재 로드 상태 반환."""
    if _nemotron_instance is not None:
        if _nemotron_instance._api_url:
            return {
                "loaded": True,
                "mode": "nim_api",
                "api_url": _nemotron_instance._api_url,
                "model_id": _nemotron_instance._model_id,
            }
        return {
            "loaded": True,
            "mode": "local",
            "model_path": _nemotron_instance.model_path,
        }
    return {"loaded": False, "error": _nemotron_error}


# ── 프롬프트 상수 ──────────────────────────────────────────────────────────────
_PROMPT_EN = """\
This image is a page from an inverter Modbus protocol document showing register tables.
Extract ALL register definitions from the tables and return as a JSON array.

Each item must have these fields:
- "address": hex string starting with "0x" (e.g. "0x3000"). Skip if no address.
- "raw_name": register name, UPPER_SNAKE_CASE preferred.
- "data_type": one of "U16", "S16", "U32", "S32", "STRING". Default "U16" if unclear.
- "scale": float or int (e.g. 0.1). Use 1 if not shown.
- "unit": "V", "A", "W", "kWh", "Hz", "degC", "%" or "". Empty if not shown.
- "fc": 3 or 4. Default 3 if unclear.
- "rw": "R" or "RW". Default "R" if unclear.
- "description": short English description.

Output ONLY the JSON array. No markdown, no explanation.
{context}
"""

_PROMPT_MULTI = """\
This image is a page from an inverter Modbus protocol document showing register tables.
The labels may be in English, Korean, Japanese, or Chinese.
Extract ALL register definitions from the tables and return as a JSON array.

Each item must have these fields:
- "address": hex string starting with "0x" (e.g. "0x3000"). Skip if no address.
- "raw_name": register name translated to UPPER_SNAKE_CASE English (translate Korean/Japanese names).
- "data_type": one of "U16", "S16", "U32", "S32", "STRING". Default "U16" if unclear.
- "scale": float or int (e.g. 0.1). Use 1 if not shown.
- "unit": "V", "A", "W", "kWh", "Hz", "degC", "%" or "". Empty if not shown.
- "fc": 3 or 4. Default 3 if unclear.
- "rw": "R" or "RW". Default "R" if unclear.
- "description": short English description.

Korean/Japanese unit hints: V(볼트/V), A(암페어/A), W(와트/W), kWh(킬로와트시), Hz(헤르츠/Hz).
Output ONLY the JSON array. No markdown, no explanation.
{context}
"""


# ── 메인 클래스 ───────────────────────────────────────────────────────────────
class NemotronOCRModel:
    """Nemotron OCR v2 추론 래퍼.

    api_url + api_key 지정 시 NVIDIA NIM API 클라이언트 모드.
    없으면 로컬 HuggingFace VL 모델 로드 모드.
    """

    def __init__(
        self,
        model_path: str = "",
        device: str = "auto",
        api_url: str = "",
        api_key: str = "",
        model_id: str = "nvidia/llama-3.1-nemotron-nano-vl-8b-v1",
    ):
        self.model_path = model_path
        self._api_url = api_url.rstrip("/") if api_url else ""
        self._api_key = api_key
        self._model_id = model_id

        # ── NIM API 클라이언트 모드 ──────────────────────────────────────────
        if self._api_url:
            logger.info("[NemotronOCR] NIM API 모드: %s  model=%s", self._api_url, self._model_id)
            self.model = None
            self.processor = None
            return

        # ── 로컬 HuggingFace 로드 모드 ────────────────────────────────────
        if not model_path:
            raise ValueError("[NemotronOCR] api_url 또는 model_path 중 하나가 필요합니다.")

        logger.info("[NemotronOCR] 로컬 모델 로드 중: %s (device=%s)", model_path, device)
        import transformers as _tf

        _model_cls = None
        for _cls_name in (
            "LlavaNextForConditionalGeneration",
            "LlavaForConditionalGeneration",
            "AutoModelForVision2Seq",
        ):
            _cls = getattr(_tf, _cls_name, None)
            if _cls is not None:
                _model_cls = _cls
                break

        if _model_cls is None:
            from transformers import AutoModelForVision2Seq
            _model_cls = AutoModelForVision2Seq

        from transformers import AutoProcessor
        self.processor = AutoProcessor.from_pretrained(model_path, trust_remote_code=True)
        self.model = _model_cls.from_pretrained(
            model_path,
            device_map=device,
            torch_dtype="auto",
            trust_remote_code=True,
        )
        self.model.eval()
        logger.info("[NemotronOCR] 로드 완료")

    # ── 내부 헬퍼 ─────────────────────────────────────────────────────────────
    def _infer_image(self, img_bytes: bytes, prompt: str, max_new_tokens: int = 4096) -> str:
        """이미지 bytes + 텍스트 프롬프트 -> 모델 생성 텍스트."""
        # ── NIM API 모드 ───────────────────────────────────────────────────
        if self._api_url:
            import requests
            image_b64 = base64.b64encode(img_bytes).decode("utf-8")
            payload = {
                "model": self._model_id,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/png;base64,{image_b64}"},
                            },
                            {"type": "text", "text": prompt},
                        ],
                    }
                ],
                "max_tokens": max_new_tokens,
                "temperature": 0.0,
            }
            headers = {"Content-Type": "application/json"}
            if self._api_key:
                headers["Authorization"] = f"Bearer {self._api_key}"

            resp = requests.post(
                f"{self._api_url}/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=300,
            )
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]

        # ── 로컬 HuggingFace 모드 ──────────────────────────────────────────
        import torch
        from PIL import Image

        image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image", "image": image},
                    {"type": "text", "text": prompt},
                ],
            }
        ]
        text = self.processor.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )
        inputs = self.processor(text=[text], images=[image], return_tensors="pt").to(
            self.model.device
        )
        with torch.no_grad():
            output_ids = self.model.generate(
                **inputs,
                max_new_tokens=max_new_tokens,
                do_sample=False,
            )
        input_len = inputs["input_ids"].shape[1] if "input_ids" in inputs else 0
        gen_ids = output_ids[0][input_len:]
        return self.processor.tokenizer.decode(gen_ids, skip_special_tokens=True).strip()

    @staticmethod
    def _extract_json(text: str) -> Any:
        """응답 텍스트에서 JSON 배열 파싱. 실패 시 None."""
        m = re.search(r"```(?:json)?\s*(\[.*?)\s*```", text, re.DOTALL)
        if m:
            text = m.group(1)
        else:
            start = text.find("[")
            end = text.rfind("]")
            if 0 <= start < end:
                text = text[start : end + 1]
        try:
            return json.loads(text)
        except Exception:
            return None

    # ── 공개 메서드 ───────────────────────────────────────────────────────────
    def extract_table_from_image(
        self, img_bytes: bytes, context: str = "", lang: str = "en"
    ) -> list[dict]:
        """단일 페이지 이미지 -> 레지스터 dict 목록."""
        ctx_line = f"\nAdditional context: {context}" if context else ""
        template = _PROMPT_MULTI if lang == "multi" else _PROMPT_EN
        prompt = template.format(context=ctx_line)
        try:
            raw = self._infer_image(img_bytes, prompt, max_new_tokens=4096)
            parsed = self._extract_json(raw)
            if isinstance(parsed, list):
                return parsed
            logger.warning("[NemotronOCR.extract_table_from_image] JSON 파싱 실패")
            return []
        except Exception as e:
            logger.warning("[NemotronOCR.extract_table_from_image] 오류: %s", e)
            return []

    def extract_regions_from_image(
        self,
        img_bytes: bytes,
        page_idx: int = 0,
        source: str = "",
        lang: str = "en",
    ) -> list[OcrRegion]:
        """단일 이미지 -> 원문 텍스트 OcrRegion 목록 (bbox/confidence는 None).

        모델이 JSON 레지스터 배열을 반환하므로 전체 응답을 단일 region으로 감싼다.
        """
        if not source:
            source = "nim_api" if self._api_url else "local"
        ctx_line = ""
        template = _PROMPT_MULTI if lang == "multi" else _PROMPT_EN
        prompt = template.format(context=ctx_line)
        try:
            raw = self._infer_image(img_bytes, prompt, max_new_tokens=4096)
        except Exception as e:
            logger.warning("[NemotronOCR.extract_regions_from_image] 오류: %s", e)
            raw = ""
        return [OcrRegion(text=raw, confidence=None, bbox=None, page=page_idx, source=source)]

    def extract_pages_from_dir(
        self,
        fixture_dir: str,
        lang: str = "en",
        log=None,
    ) -> list[dict]:
        """PNG fixture 디렉터리에서 직접 레지스터 추출.

        fixture_dir 아래 page_*.png 파일을 정렬 순서로 처리.
        """
        png_files = sorted(
            f for f in os.listdir(fixture_dir) if f.endswith(".png")
        )
        if not png_files:
            logger.warning("[NemotronOCR.extract_pages_from_dir] PNG 없음: %s", fixture_dir)
            return []

        merged: dict[str, dict] = {}
        total = len(png_files)
        for i, fname in enumerate(png_files):
            page_idx = i
            fpath = os.path.join(fixture_dir, fname)
            if log:
                log(f"[NemotronOCR] fixture {fname} ({i + 1}/{total}) OCR 중...")
            try:
                with open(fpath, "rb") as f:
                    img_bytes = f.read()
            except Exception as e:
                logger.warning("[NemotronOCR.extract_pages_from_dir] 파일 읽기 실패: %s", e)
                continue

            regs = self.extract_table_from_image(img_bytes, lang=lang)
            if log:
                log(f"[NemotronOCR] fixture {fname}: {len(regs)}개 항목 추출")
            for reg in regs:
                addr = str(reg.get("address", "")).strip().lower()
                if addr:
                    merged[addr] = reg

        return list(merged.values())

    def extract_pages(
        self,
        pdf_path: str,
        page_indices: list[int],
        dpi: int = 200,
        lang: str = "en",
        log=None,
    ) -> list[dict]:
        """여러 PDF 페이지를 순차 처리, 주소 기준 중복 제거 후 반환.

        Qwen과 달리 sparse 필터 없이 지정된 모든 페이지를 처리한다.
        """
        from .ai_qwen_vl import render_pdf_page  # PyMuPDF 래퍼 공유

        merged: dict[str, dict] = {}
        total = len(page_indices)

        for i, idx in enumerate(page_indices):
            if log:
                log(f"[NemotronOCR] 페이지 {idx + 1} ({i + 1}/{total}) OCR 중...")
            try:
                img_bytes = render_pdf_page(pdf_path, idx, dpi=dpi)
            except Exception as e:
                logger.warning("[NemotronOCR.extract_pages] 페이지 %d 렌더링 실패: %s", idx, e)
                continue

            regs = self.extract_table_from_image(img_bytes, lang=lang)
            if log:
                log(f"[NemotronOCR] 페이지 {idx + 1}: {len(regs)}개 항목 추출")

            for reg in regs:
                addr = str(reg.get("address", "")).strip().lower()
                if addr:
                    merged[addr] = reg

        return list(merged.values())
