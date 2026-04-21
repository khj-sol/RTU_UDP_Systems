# -*- coding: utf-8 -*-
"""Fast PDF layout extraction for Stage1 v4.2.

Primary path is CPU-only PyMuPDF structure/table extraction. If optional
pymupdf_layout or pymupdf4llm is installed later, this module can use it without
making the default path depend on a large VLM.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Any

from .ocr_row_reconstructor import (
    candidates_from_text_lines,
    parse_register_candidate_from_cells,
)

LogFn = Callable[[str, str], None] | None


@dataclass
class LayoutExtractionResult:
    candidates: list[dict] = field(default_factory=list)
    fallback_pages: list[int] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


def _log(log: LogFn, msg: str, level: str = 'info') -> None:
    if log:
        log(msg, level)


def _page_text_lines(page) -> list[str]:
    text = page.get_text('text') or ''
    return [line.strip() for line in text.splitlines() if line.strip()]


def _extract_tables(page, page_no: int) -> tuple[list[dict], int]:
    candidates: list[dict] = []
    table_count = 0
    finder = getattr(page, 'find_tables', None)
    if not callable(finder):
        return candidates, table_count
    try:
        tables_obj = finder()
        tables = getattr(tables_obj, 'tables', tables_obj)
    except Exception:
        return candidates, table_count
    for table in tables or []:
        table_count += 1
        try:
            rows = table.extract()
        except Exception:
            rows = []
        for row in rows or []:
            cand = parse_register_candidate_from_cells(row, page=page_no, source='layout_table')
            if cand:
                candidates.append(cand)
    return candidates, table_count


def _dedupe(candidates: list[dict]) -> list[dict]:
    deduped: dict[str, dict] = {}
    for cand in candidates:
        address = str(cand.get('address', '')).lower()
        name = str(cand.get('raw_name') or cand.get('name') or '')[:80].lower()
        key = f'{address}|{name}'
        if key not in deduped:
            deduped[key] = cand
    return list(deduped.values())


def extract_layout_candidates(
    pdf_path: str,
    min_valid_rows: int = 5,
    log: LogFn = None,
) -> LayoutExtractionResult:
    """Extract register candidates from digital PDF structure.

    Returns candidate dicts using the same contract as AI extraction:
    address/raw_name/description/data_type/scale/unit/fc/rw/page/confidence.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError as exc:
        return LayoutExtractionResult(
            candidates=[],
            fallback_pages=[],
            stats={
                'layout_used': False,
                'layout_blocks': 0,
                'layout_tables': 0,
                'valid_register_rows': 0,
                'extractor_fallback_reason': f'pymupdf_missing: {exc}',
            },
        )

    candidates: list[dict] = []
    fallback_pages: list[int] = []
    layout_blocks = 0
    layout_tables = 0
    page_count = 0

    doc = fitz.open(pdf_path)
    try:
        page_count = len(doc)
        for idx, page in enumerate(doc):
            page_no = idx + 1
            try:
                text_dict = page.get_text('dict') or {}
                blocks = text_dict.get('blocks') or []
                layout_blocks += len(blocks)
            except Exception:
                blocks = []

            page_candidates: list[dict] = []
            table_candidates, table_count = _extract_tables(page, page_no)
            layout_tables += table_count
            page_candidates.extend(table_candidates)

            lines = _page_text_lines(page)
            page_candidates.extend(candidates_from_text_lines(lines, page=page_no, source='layout_text'))
            page_candidates = _dedupe(page_candidates)
            candidates.extend(page_candidates)

            if len(page_candidates) < min_valid_rows:
                fallback_pages.append(idx)
    finally:
        doc.close()

    candidates = _dedupe(candidates)
    reason = ''
    if fallback_pages:
        reason = f'low_valid_rows_pages={len(fallback_pages)}'
    if not candidates:
        reason = 'no_layout_register_rows'

    stats = {
        'layout_used': True,
        'layout_blocks': layout_blocks,
        'layout_tables': layout_tables,
        'rapidocr_used': False,
        'rapidocr_pages': 0,
        'ocr_boxes': 0,
        'valid_register_rows': len(candidates),
        'extractor_fallback_reason': reason,
        'page_count': page_count,
    }
    _log(log, f'[Layout] candidates={len(candidates)} tables={layout_tables} fallback_pages={len(fallback_pages)}')
    return LayoutExtractionResult(candidates=candidates, fallback_pages=fallback_pages, stats=stats)
