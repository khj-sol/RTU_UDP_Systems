# -*- coding: utf-8 -*-
"""RapidOCR ONNX adapter for Stage1 v4.2.

The adapter is optional and lazy-loaded. Digital PDFs can complete with layout
extraction only; RapidOCR is used for image/scanned pages or explicit
rapidocr_only benchmarks.
"""
from __future__ import annotations

import glob
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Callable

from .ocr_row_reconstructor import reconstruct_register_candidates

LogFn = Callable[[str, str], None] | None

_ENGINE = None
_ENGINE_ERROR: str | None = None


@dataclass
class RapidOCRResult:
    candidates: list[dict]
    stats: dict[str, Any]


def _log(log: LogFn, msg: str, level: str = 'info') -> None:
    if log:
        log(msg, level)


def _import_rapidocr():
    try:
        from rapidocr_onnxruntime import RapidOCR
        return RapidOCR
    except Exception:
        try:
            from rapidocr import RapidOCR
            return RapidOCR
        except Exception as exc:
            raise RuntimeError(
                'RapidOCR package is not installed. Install rapidocr-onnxruntime '
                'and configure model paths if needed.'
            ) from exc


def get_rapidocr_status() -> dict:
    return {
        'loaded': _ENGINE is not None,
        'error': _ENGINE_ERROR,
    }


def get_rapidocr_engine(settings: dict | None = None):
    global _ENGINE, _ENGINE_ERROR
    if _ENGINE is not None:
        return _ENGINE
    settings = settings or {}
    try:
        RapidOCR = _import_rapidocr()
        kwargs: dict[str, Any] = {}
        path_map = {
            'det_model_path': settings.get('rapidocr_det_model_path') or settings.get('det_model_path'),
            'rec_model_path': settings.get('rapidocr_rec_model_path') or settings.get('rec_model_path'),
            'rec_keys_path': settings.get('rapidocr_rec_keys_path') or settings.get('rec_keys_path'),
        }
        for key, value in path_map.items():
            if value:
                kwargs[key] = value
        try:
            _ENGINE = RapidOCR(**kwargs)
        except TypeError:
            # Older RapidOCR constructors use no keyword config.
            _ENGINE = RapidOCR()
        _ENGINE_ERROR = None
        return _ENGINE
    except Exception as exc:
        _ENGINE = None
        _ENGINE_ERROR = str(exc)
        raise


def _normalize_result(raw: Any, page: int) -> list[dict]:
    # Common RapidOCR shapes:
    #   result, _ = engine(path)
    #   result = [(box, text, score), ...]
    #   result = [{'dt_boxes':..., 'rec_text':..., 'score':...}, ...]
    if raw is None:
        return []
    if isinstance(raw, tuple) and raw:
        raw = raw[0]
    if isinstance(raw, dict):
        if 'text' in raw:
            raw = [raw]
        else:
            raw = raw.get('result') or raw.get('results') or raw.get('ocr_result') or []
    regions: list[dict] = []
    for item in raw or []:
        text = ''
        score = None
        bbox = None
        if isinstance(item, dict):
            text = item.get('text') or item.get('rec_text') or item.get('label') or ''
            score = item.get('confidence') or item.get('score') or item.get('rec_score')
            bbox = item.get('bbox') or item.get('box') or item.get('dt_box') or item.get('points')
        elif isinstance(item, (list, tuple)):
            if len(item) >= 3:
                bbox, text, score = item[0], item[1], item[2]
            elif len(item) == 2:
                bbox, text = item[0], item[1]
        if not text:
            continue
        regions.append({'text': str(text), 'confidence': score or 0.0, 'bbox': bbox or [], 'page': page, 'source': 'rapidocr'})
    return regions


def extract_image_regions(image_path: str, settings: dict | None = None, page: int = 1, log: LogFn = None) -> list[dict]:
    engine = get_rapidocr_engine(settings)
    raw = engine(image_path)
    regions = _normalize_result(raw, page=page)
    _log(log, f'[RapidOCR] page={page} boxes={len(regions)} image={os.path.basename(image_path)}')
    return regions


def extract_pdf_pages(
    pdf_path: str,
    page_indices: list[int] | None = None,
    settings: dict | None = None,
    dpi: int = 200,
    log: LogFn = None,
) -> RapidOCRResult:
    settings = settings or {}
    try:
        import fitz
    except ImportError as exc:
        return RapidOCRResult([], {'rapidocr_used': False, 'rapidocr_pages': 0, 'ocr_boxes': 0, 'valid_register_rows': 0, 'extractor_fallback_reason': f'pymupdf_missing_for_render: {exc}'})

    candidates: list[dict] = []
    total_boxes = 0
    pages_done = 0
    doc = fitz.open(pdf_path)
    try:
        indices = page_indices if page_indices is not None else list(range(len(doc)))
        matrix = fitz.Matrix(dpi / 72.0, dpi / 72.0)
        with tempfile.TemporaryDirectory(prefix='rapidocr_pages_') as tmp:
            for idx in indices:
                if idx < 0 or idx >= len(doc):
                    continue
                page_no = idx + 1
                image_path = os.path.join(tmp, f'page_{idx:04d}.png')
                pix = doc[idx].get_pixmap(matrix=matrix, alpha=False)
                pix.save(image_path)
                regions = extract_image_regions(image_path, settings=settings, page=page_no, log=log)
                total_boxes += len(regions)
                pages_done += 1
                candidates.extend(reconstruct_register_candidates(regions, page=page_no, source='rapidocr'))
    finally:
        doc.close()

    stats = {
        'rapidocr_used': pages_done > 0,
        'rapidocr_pages': pages_done,
        'ocr_boxes': total_boxes,
        'valid_register_rows': len(candidates),
        'extractor_fallback_reason': '',
    }
    return RapidOCRResult(candidates=candidates, stats=stats)


def extract_fixture_candidates(fixtures_dir: str, settings: dict | None = None, log: LogFn = None) -> RapidOCRResult:
    image_paths = sorted(
        glob.glob(os.path.join(fixtures_dir, '*.png')) +
        glob.glob(os.path.join(fixtures_dir, '*.jpg')) +
        glob.glob(os.path.join(fixtures_dir, '*.jpeg'))
    )
    candidates: list[dict] = []
    total_boxes = 0
    for idx, image_path in enumerate(image_paths, start=1):
        regions = extract_image_regions(image_path, settings=settings, page=idx, log=log)
        total_boxes += len(regions)
        candidates.extend(reconstruct_register_candidates(regions, page=idx, source='rapidocr_fixture'))
    return RapidOCRResult(
        candidates=candidates,
        stats={
            'rapidocr_used': bool(image_paths),
            'rapidocr_pages': len(image_paths),
            'ocr_boxes': total_boxes,
            'valid_register_rows': len(candidates),
            'extractor_fallback_reason': '' if image_paths else 'no_image_fixtures',
        },
    )
