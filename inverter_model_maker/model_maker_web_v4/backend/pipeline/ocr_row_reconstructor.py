# -*- coding: utf-8 -*-
"""OCR bbox/text rows -> Modbus register candidates.

This module is intentionally small and deterministic.  OCR/layout engines only
produce text boxes or table cells; matching is still handled by Stage1 rules,
synonym_db, definitions, and the reference map.
"""
from __future__ import annotations

import re
from typing import Any, Iterable

_ADDRESS_RE = re.compile(r'(?<![A-Za-z0-9])(?:0x[0-9A-Fa-f]{3,5}|[0-9A-Fa-f]{4}[Hh]|[34]\d{4}|\d{4,5})(?![A-Za-z0-9])')
_TYPE_RE = re.compile(r'\b(U16|S16|U32|S32|UINT16|INT16|UINT32|INT32|FLOAT32|FLOAT|STRING|ASCII|BITFIELD)\b', re.I)
_FC_RE = re.compile(r'\b(?:FC|Function\s*Code|Function)\s*0?([1346])\b', re.I)
_SCALE_RE = re.compile(r'(?:scale|factor|gain|ratio|unit\s*gain)?\s*(?:[:=xX*/]\s*)?(0\.\d+|\d+\.\d+|\d+)', re.I)
_UNIT_RE = re.compile(r'(?<![A-Za-z])(?:kWh|Wh|kW|W|Var|kVar|VAr|Hz|degC|℃|°C|V|A|%|PF)(?![A-Za-z])', re.I)
_RW_RE = re.compile(r'\b(RW|RO|WO|R/W|Read/Write|Read\s*Only|Write\s*Only|Read|Write)\b', re.I)
_NOISE_RE = re.compile(r'\b(address|addr|register|reg|modbus|data|type|scale|unit|description|remarks?|range|function|code|fc|read|write|rw|ro|wo)\b', re.I)


def normalize_text(value: Any) -> str:
    text = '' if value is None else str(value)
    text = text.replace('\u00a0', ' ').replace('\t', ' ')
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_address(value: str) -> tuple[int | None, str]:
    text = normalize_text(value)
    m = _ADDRESS_RE.search(text)
    if not m:
        return None, ''
    raw = m.group(0)
    try:
        if raw.lower().startswith('0x'):
            addr = int(raw, 16)
        elif raw.lower().endswith('h'):
            addr = int(raw[:-1], 16)
        else:
            addr = int(raw, 10)
    except ValueError:
        try:
            addr = int(raw, 16)
        except ValueError:
            return None, raw
    if not (0 <= addr <= 0xFFFF):
        return None, raw
    return addr, raw


def _detect_type(text: str) -> str:
    m = _TYPE_RE.search(text)
    if not m:
        return 'U16'
    value = m.group(1).upper()
    aliases = {
        'UINT16': 'U16', 'INT16': 'S16', 'UINT32': 'U32', 'INT32': 'S32',
        'FLOAT': 'FLOAT32', 'ASCII': 'STRING',
    }
    return aliases.get(value, value)


def _detect_fc(text: str, rw: str) -> int:
    m = _FC_RE.search(text)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    return 3 if rw in ('RW', 'WO') else 4


def _detect_rw(text: str) -> str:
    m = _RW_RE.search(text)
    if not m:
        return 'R'
    raw = m.group(1).lower().replace(' ', '')
    if raw in ('rw', 'r/w', 'read/write'):
        return 'RW'
    if raw in ('wo', 'writeonly', 'write'):
        return 'WO'
    return 'R'


def _detect_unit(text: str) -> str:
    m = _UNIT_RE.search(text)
    if not m:
        return ''
    unit = m.group(0)
    return {'℃': 'degC', '°C': 'degC'}.get(unit, unit)


def _detect_scale(text: str) -> str:
    lower = text.lower()
    explicit = re.search(r'(?:scale|factor|gain|ratio)\s*[:=]?\s*(0\.\d+|\d+\.\d+|\d+)', lower)
    if explicit:
        return explicit.group(1)
    div = re.search(r'/(10|100|1000|10000)\b', lower)
    if div:
        denom = int(div.group(1))
        return f'{1 / denom:g}'
    mul = re.search(r'[xX]\s*(0\.\d+|\d+\.\d+)', text)
    if mul:
        return mul.group(1)
    return '1'


def _clean_name(text: str, address_token: str) -> str:
    cleaned = normalize_text(text.replace(address_token, ' '))
    cleaned = _TYPE_RE.sub(' ', cleaned)
    cleaned = _FC_RE.sub(' ', cleaned)
    cleaned = _RW_RE.sub(' ', cleaned)
    cleaned = re.sub(r'(?:scale|factor|gain|ratio)\s*[:=]?\s*(0\.\d+|\d+\.\d+|\d+)', ' ', cleaned, flags=re.I)
    cleaned = _NOISE_RE.sub(' ', cleaned)
    cleaned = re.sub(r'\b(0\.\d+|\d+\.\d+)\b', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip(' -_:;,')
    if len(cleaned) < 2:
        cleaned = normalize_text(text)
    return cleaned[:120] or 'REGISTER'


def parse_register_candidate_from_text(text: str, page: int = 0, source: str = 'layout', confidence: float | None = None) -> dict | None:
    text = normalize_text(text)
    addr, raw_addr = parse_address(text)
    if addr is None:
        return None
    rw = _detect_rw(text)
    raw_name = _clean_name(text, raw_addr)
    candidate = {
        'address': f'0x{addr:04X}',
        'raw_name': raw_name,
        'name': raw_name,
        'description': text[:300],
        'data_type': _detect_type(text),
        'scale': _detect_scale(text),
        'unit': _detect_unit(text),
        'fc': _detect_fc(text, rw),
        'rw': rw,
        'page': page,
        'source': source,
    }
    if confidence is not None:
        candidate['confidence'] = confidence
    return candidate


def parse_register_candidate_from_cells(cells: Iterable[Any], page: int = 0, source: str = 'layout_table') -> dict | None:
    values = [normalize_text(c) for c in cells if normalize_text(c)]
    if not values:
        return None
    joined = ' | '.join(values)
    return parse_register_candidate_from_text(joined, page=page, source=source)


def candidates_from_text_lines(lines: Iterable[str], page: int = 0, source: str = 'layout_text') -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for line in lines:
        cand = parse_register_candidate_from_text(line, page=page, source=source)
        if not cand:
            continue
        key = cand['address'] + '|' + cand.get('raw_name', '')[:40]
        if key in seen:
            continue
        seen.add(key)
        out.append(cand)
    return out


def _bbox_y_center(region: dict) -> float:
    bbox = region.get('bbox') or region.get('box') or []
    if not bbox:
        return 0.0
    if len(bbox) == 4 and all(isinstance(v, (int, float)) for v in bbox):
        return (float(bbox[1]) + float(bbox[3])) / 2.0
    ys = []
    for point in bbox:
        if isinstance(point, (list, tuple)) and len(point) >= 2:
            ys.append(float(point[1]))
    return sum(ys) / len(ys) if ys else 0.0


def _bbox_x_min(region: dict) -> float:
    bbox = region.get('bbox') or region.get('box') or []
    if not bbox:
        return 0.0
    if len(bbox) == 4 and all(isinstance(v, (int, float)) for v in bbox):
        return float(bbox[0])
    xs = []
    for point in bbox:
        if isinstance(point, (list, tuple)) and len(point) >= 1:
            xs.append(float(point[0]))
    return min(xs) if xs else 0.0


def reconstruct_rows_from_ocr_regions(regions: Iterable[dict], y_tolerance: float = 12.0) -> list[list[dict]]:
    sorted_regions = sorted(regions, key=lambda r: (_bbox_y_center(r), _bbox_x_min(r)))
    rows: list[list[dict]] = []
    row_centers: list[float] = []
    for region in sorted_regions:
        y = _bbox_y_center(region)
        target = None
        for idx, center in enumerate(row_centers):
            if abs(center - y) <= y_tolerance:
                target = idx
                break
        if target is None:
            rows.append([region])
            row_centers.append(y)
        else:
            rows[target].append(region)
            row_centers[target] = (row_centers[target] + y) / 2.0
    for row in rows:
        row.sort(key=_bbox_x_min)
    return rows


def reconstruct_register_candidates(regions: Iterable[dict], page: int = 0, source: str = 'rapidocr') -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for row in reconstruct_rows_from_ocr_regions(regions):
        texts = [normalize_text(r.get('text', '')) for r in row if normalize_text(r.get('text', ''))]
        if not texts:
            continue
        confidence_values = [float(r.get('confidence', 0.0)) for r in row if isinstance(r.get('confidence'), (int, float))]
        confidence = sum(confidence_values) / len(confidence_values) if confidence_values else None
        cand = parse_register_candidate_from_text(' | '.join(texts), page=page, source=source, confidence=confidence)
        if not cand:
            continue
        key = cand['address'] + '|' + cand.get('raw_name', '')[:40]
        if key in seen:
            continue
        seen.add(key)
        out.append(cand)
    return out
