# -*- coding: utf-8 -*-
"""
Qwen3-VL-32B-4bit (HuggingFace) 래퍼 — V4 하이브리드 AI 파이프라인

역할:
  1. render_pdf_page()          : PDF 페이지 → PNG bytes (PyMuPDF)
  2. extract_table_from_image() : 단일 이미지 → 레지스터 JSON 배열
  3. extract_pages()            : 여러 페이지 순차 처리 + 주소 기준 중복 제거

호출처:
  - stage1.py  _run_hybrid_ai_extraction()
  - api_routes.py  /ai/status, /ai/load
"""
from __future__ import annotations

import base64
import io
import json
import re
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── 싱글톤 ────────────────────────────────────────────────────────────────────
_qwen_instance: "QwenVLModel | None" = None
_qwen_error: str = ""


def get_qwen_vl_model(
    model_path: str,
    device: str = "auto",
    wsl_server_url: str = "",
) -> "QwenVLModel | None":
    """QwenVLModel 싱글톤. 이미 로드됐으면 재사용. 실패하면 None 반환.

    wsl_server_url이 있으면 WSL HTTP 클라이언트 모드 (모델 로컬 로드 없음).
    없으면 로컬 HuggingFace 로드 모드 (fallback).
    """
    global _qwen_instance, _qwen_error
    if _qwen_instance is not None:
        return _qwen_instance
    try:
        _qwen_instance = QwenVLModel(model_path, device, wsl_server_url=wsl_server_url)
        _qwen_error = ""
        return _qwen_instance
    except Exception as e:
        _qwen_error = str(e)
        logger.warning("[QwenVL] 로드 실패: %s", e)
        return None


def get_qwen_vl_status() -> dict:
    """현재 로드 상태 반환 (api_routes용)."""
    if _qwen_instance is not None:
        # WSL 클라이언트 모드: /health 엔드포인트 조회
        if _qwen_instance._wsl_url:
            try:
                import requests
                r = requests.get(
                    f"{_qwen_instance._wsl_url}/health", timeout=3
                )
                data = r.json()
                data["mode"] = "wsl_client"
                return data
            except Exception as e:
                return {
                    "loaded": False,
                    "mode": "wsl_client",
                    "error": f"WSL 서버 응답 없음: {e}",
                }
        # 로컬 모드
        try:
            dev = str(next(_qwen_instance.model.parameters()).device)
        except Exception:
            dev = "unknown"
        return {
            "loaded": True,
            "model_path": _qwen_instance.model_path,
            "device": dev,
            "mode": "local",
        }
    return {"loaded": False, "error": _qwen_error}


# ── PDF → 이미지 헬퍼 ─────────────────────────────────────────────────────────
def render_pdf_page(pdf_path: str, page_index: int, dpi: int = 200) -> bytes:
    """PyMuPDF로 PDF 페이지를 PNG bytes로 렌더링."""
    import fitz  # PyMuPDF — V3에서 이미 사용 중

    doc = fitz.open(pdf_path)
    try:
        page = doc[page_index]
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat)
        return pix.tobytes("png")
    finally:
        doc.close()


# ── 프롬프트 상수 ──────────────────────────────────────────────────────────────
_PROMPT_EXTRACT = """\
이 이미지는 인버터 Modbus 프로토콜 문서의 레지스터 표입니다.
표에서 모든 레지스터 정의를 추출해 JSON 배열로 반환하세요.

각 항목의 필드:
- "address": "0x" 로 시작하는 hex 문자열 (예: "0x3000"). 주소 없으면 건너뜀.
- "raw_name": 레지스터 이름, UPPER_SNAKE_CASE 선호.
- "data_type": "U16", "S16", "U32", "S32", "STRING" 중 하나. 불분명하면 "U16".
- "scale": 숫자(float 또는 int). 없으면 1.
- "unit": "V", "A", "W", "kWh", "Hz", "degC", "%" 또는 "". 없으면 "".
- "fc": 3 또는 4. 불분명하면 3.
- "rw": "R" 또는 "RW". 없으면 "R".
- "description": 짧은 영문 설명.

JSON 배열만 출력하세요. 설명이나 마크다운 코드블록 없이.
{context}
"""


# ── 메인 클래스 ───────────────────────────────────────────────────────────────
class QwenVLModel:
    """HuggingFace Qwen3-VL (또는 호환 VL) 추론 래퍼.

    wsl_server_url이 지정되면 WSL HTTP 클라이언트 모드로 동작.
    모델을 로컬에 로드하지 않고 WSL 서버의 /extract 엔드포인트를 호출.
    """

    def __init__(self, model_path: str, device: str = "auto", wsl_server_url: str = ""):
        self.model_path = model_path
        self._wsl_url = wsl_server_url.rstrip("/") if wsl_server_url else ""

        # ── WSL 클라이언트 모드 ───────────────────────────────────────────────
        if self._wsl_url:
            logger.info("[QwenVL] WSL 클라이언트 모드: %s", self._wsl_url)
            self.model = None
            self.processor = None
            self._use_qwen_processor = False
            return

        # ── 로컬 로드 모드 ────────────────────────────────────────────────────
        logger.info("[QwenVL] 모델 로드 중: %s (device=%s)", model_path, device)

        # Qwen3-VL 우선, Qwen2.5-VL, Qwen2-VL 순으로 시도 (transformers 5.x 클래스명 기준)
        import transformers as _tf
        _model_cls = None
        for _cls_name in (
            'Qwen3VLForConditionalGeneration',     # Qwen3-VL (transformers 5.x)
            'Qwen2_5_VLForConditionalGeneration',  # Qwen2.5-VL (transformers 5.x, 언더스코어 수정)
            'Qwen2VLForConditionalGeneration',     # Qwen2-VL
        ):
            _cls = getattr(_tf, _cls_name, None)
            if _cls is not None:
                _model_cls = _cls
                break

        if _model_cls is None:
            raise ImportError(
                "지원되는 Qwen VL 클래스를 찾을 수 없습니다 "
                "(Qwen3VLForConditionalGeneration / Qwen2_5_VLForConditionalGeneration / Qwen2VLForConditionalGeneration). "
                "transformers 버전을 확인하세요."
            )

        from transformers import AutoProcessor
        self.processor = AutoProcessor.from_pretrained(
            model_path, trust_remote_code=True
        )
        self.model = _model_cls.from_pretrained(
            model_path,
            device_map=device,
            dtype="auto",
            trust_remote_code=True,
        )
        self._use_qwen_processor = True

        self.model.eval()
        logger.info("[QwenVL] 로드 완료")

    # ── 내부 헬퍼 ─────────────────────────────────────────────────────────────
    def _infer_image(
        self, img_bytes: bytes, prompt: str, max_new_tokens: int = 4096
    ) -> str:
        """PIL Image + 텍스트 프롬프트 → 모델 생성 텍스트.

        WSL 클라이언트 모드: base64로 인코딩해 WSL 서버 /extract 호출.
        로컬 모드: HuggingFace 모델 직접 추론.
        """
        # ── WSL 클라이언트 모드 ───────────────────────────────────────────────
        if self._wsl_url:
            import requests
            image_b64 = base64.b64encode(img_bytes).decode("utf-8")
            logger.debug("[QwenVL] WSL 서버 호출: %s/extract", self._wsl_url)
            resp = requests.post(
                f"{self._wsl_url}/extract",
                json={"image_b64": image_b64, "prompt": prompt, "max_new_tokens": max_new_tokens},
                timeout=300,
            )
            resp.raise_for_status()
            return resp.json().get("text", "")

        # ── 로컬 모드 ─────────────────────────────────────────────────────────
        import torch
        from PIL import Image

        image = Image.open(io.BytesIO(img_bytes)).convert("RGB")

        if self._use_qwen_processor:
            # Qwen2.5-VL: messages 형식
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
            inputs = self.processor(
                text=[text],
                images=[image],
                return_tensors="pt",
            ).to(self.model.device)
        else:
            # 일반 AutoModelForVision2Seq 폴백
            inputs = self.processor(
                images=image,
                text=prompt,
                return_tensors="pt",
            ).to(self.model.device)

        with torch.no_grad():
            output_ids = self.model.generate(
                **inputs,
                max_new_tokens=max_new_tokens,
                do_sample=False,
                temperature=1.0,
            )

        # 입력 토큰 이후 생성된 부분만 디코딩
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
                text = text[start: end + 1]
        try:
            return json.loads(text)
        except Exception:
            return None

    # ── 공개 메서드 ───────────────────────────────────────────────────────────
    def extract_table_from_image(
        self, img_bytes: bytes, context: str = ""
    ) -> list[dict]:
        """단일 페이지 이미지 → 레지스터 dict 목록.

        파싱 실패 시 빈 목록 반환 (폴백).
        """
        ctx_line = f"\n참고 문맥: {context}" if context else ""
        prompt = _PROMPT_EXTRACT.format(context=ctx_line)
        try:
            raw = self._infer_image(img_bytes, prompt, max_new_tokens=4096)
            parsed = self._extract_json(raw)
            if isinstance(parsed, list):
                return parsed
            logger.warning("[QwenVL.extract_table_from_image] JSON 파싱 실패")
            return []
        except Exception as e:
            logger.warning("[QwenVL.extract_table_from_image] 오류: %s", e)
            return []

    def extract_pages(
        self,
        pdf_path: str,
        page_indices: list[int],
        dpi: int = 200,
        log=None,
    ) -> list[dict]:
        """여러 PDF 페이지를 순차 처리, 주소 기준 중복 제거 후 반환.

        Args:
            pdf_path:     PDF 경로
            page_indices: 처리할 페이지 인덱스 (0-based)
            dpi:          렌더링 DPI (기본 200)
            log:          진행 로그 콜백 (선택, callable(str))

        Returns:
            레지스터 dict 목록 (address 기준 중복 제거, 나중 페이지 우선)
        """
        merged: dict[str, dict] = {}  # address_lower → register

        for idx in page_indices:
            if log:
                log(f"[QwenVL] 페이지 {idx + 1} 이미지 추출 중...")
            try:
                img_bytes = render_pdf_page(pdf_path, idx, dpi=dpi)
            except Exception as e:
                logger.warning("[QwenVL.extract_pages] 페이지 %d 렌더링 실패: %s", idx, e)
                continue

            regs = self.extract_table_from_image(img_bytes)
            if log:
                log(f"[QwenVL] 페이지 {idx + 1}: {len(regs)}개 항목 추출")

            for reg in regs:
                addr = str(reg.get("address", "")).strip().lower()
                if addr:
                    merged[addr] = reg  # 나중 페이지(= 더 시각적으로 명확한 결과) 우선

        return list(merged.values())
