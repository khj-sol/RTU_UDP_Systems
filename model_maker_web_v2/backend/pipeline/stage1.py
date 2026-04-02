# -*- coding: utf-8 -*-
"""
Stage 1 — PDF/Excel → Stage 1 Excel 레지스터맵 추출 (MAPPING_RULES_V2)

V2 핵심 변경:
- H01 DER 겹침 필드 9개는 PDF 매핑 불필요 → DER 고정 주소
- pv_voltage/pv_current는 handler 계산 → PDF 매핑 불필요
- 제어 레지스터 완전 제외 (DER-AVM으로만 제어)
- H01_MATCH 시트 추가: 전체 H01 필드 매칭 상태 표시
"""
import os
import re
import json
from datetime import datetime
from typing import List, Dict, Optional

from . import (
    PROJECT_ROOT, COMMON_DIR, MODEL_MAKER_DIR, UDP_PROTOCOL_DIR,
    RegisterRow, INVERTER_MODES, DER_CONTROL_REGS, DER_MONITOR_REGS,
    CATEGORY_COLORS, MATCH_COLORS,
    H01_DER_OVERLAP_FIELDS, H01_HANDLER_COMPUTED_FIELDS, H01_PDF_REQUIRED_FIELDS,
    to_upper_snake, parse_address, match_synonym, match_synonym_fuzzy,
    detect_channel_number, detect_channel_from_ref,
    get_ref_name_by_addr, get_h01_field_from_ref,
    load_synonym_db, load_review_history, load_reference_patterns,
    get_openpyxl, ProgressCallback,
)
from .rules import (
    classify_register_with_rules, detect_info_block, detect_iv_scan_support,
    get_valid_categories, distribute_alarms, is_h01_der_overlap,
)

# ─── PDF 추출 ─────────────────────────────────────────────────────────────────

def extract_pdf_text_and_tables(pdf_path: str) -> List[dict]:
    """PyMuPDF로 PDF 페이지별 텍스트+테이블 추출"""
    import fitz
    import logging
    logging.getLogger('fitz').setLevel(logging.ERROR)
    fitz.TOOLS.mupdf_display_errors(False)
    doc = fitz.open(pdf_path)
    pages = []
    try:
        for i, page in enumerate(doc):
            text = page.get_text()
            tables = []
            for tab in page.find_tables():
                try:
                    tables.append(tab.extract())
                except Exception:
                    pass
            pages.append({'page': i + 1, 'text': text, 'tables': tables})
    finally:
        doc.close()
    return pages


def extract_excel_sheets(excel_path: str) -> Dict[str, List[List[str]]]:
    """openpyxl로 Excel 시트별 행 추출 — 헤더 반복 시 섹션별 분리"""
    openpyxl = get_openpyxl()
    wb = openpyxl.load_workbook(excel_path, data_only=True)
    result = {}
    # 헤더 감지 키워드
    _header_keywords = {'fc', 'address', 'addr', 'parameter', 'name', 'definition',
                        'unit', 'r/w', 'type', '속성', '주소', '이름'}
    for name in wb.sheetnames:
        ws = wb[name]
        sections = []
        current_section = []
        for row in ws.iter_rows(values_only=True):
            if not any(c is not None for c in row):
                continue
            cells = [str(c) if c is not None else '' for c in row]
            # 헤더 행 감지 (3개 이상 키워드 매칭)
            cell_lower = [c.lower().strip() for c in cells if c.strip()]
            header_hits = sum(1 for c in cell_lower if any(k in c for k in _header_keywords))
            if header_hits >= 3 and current_section:
                # 이전 섹션 저장, 새 섹션 시작
                sections.append(current_section)
                current_section = []
            current_section.append(cells)
        if current_section:
            sections.append(current_section)
        # 각 섹션을 별도 테이블로
        for i, sec in enumerate(sections):
            key = f'{name}_{i}' if len(sections) > 1 else name
            result[key] = sec
    wb.close()
    return result


# ─── 레지스터 테이블 행 파싱 ──────────────────────────────────────────────────

_ADDR_RE = re.compile(r'(?:0x|0X)?([0-9A-Fa-f]{4})[Hh]?')
_TYPE_RE = re.compile(r'\b(U16|S16|U32|S32|US16|US32|I16|I32|INT16|UINT16|INT32|UINT32|FLOAT32|ASCII|STRING|STR|Bitfield16|Bitfield32)\b', re.I)
_RW_RE   = re.compile(r'\b(R/?W|RO|RD|WO|WR|Read|Write|R/W)\b', re.I)
_SCALE_RE = re.compile(r'(?:scale|factor|×|x)\s*[=:]?\s*([\d.]+)', re.I)
_UNIT_RE = re.compile(r'\b(V|A|W|kW|KW|VA|kVA|KVA|VAr|kVAr|KVar|Hz|°C|℃|Wh|kWh|KWh|Kwh|KWH|MWh|MWH|%)\b')


def _detect_table_columns(header_row: list, data_rows: list = None) -> dict:
    """테이블 헤더에서 컬럼 인덱스 추측"""
    col_map = {}
    for i, cell in enumerate(header_row):
        if not cell:
            continue
        cl = str(cell).lower().strip()
        # PDF 줄바꿈 후 공백이 삽입된 경우 대비: 공백 제거 버전도 체크
        cl_nospace = cl.replace(' ', '')
        if cl in ('address', 'addr', '주소', 'offset') or \
           ('addr' in cl and 'register' not in cl and 'reg.' not in cl) or \
           cl_nospace in ('address', 'addr'):
            col_map.setdefault('addr', i)
        elif cl.startswith('reg.') or cl == 'reg.addr':
            col_map['addr'] = i
        elif any(k in cl for k in ['name', 'definition', '이름', '항목', 'parameter', 'description',
                                    'signal name', 'signalname']) or \
             any(k in cl_nospace for k in ['signalname', 'parametername']):
            col_map.setdefault('name', i)
        elif cl == 'field' or cl == '필드':
            col_map.setdefault('name', i)
        elif any(k in cl for k in ['data type', 'datatype', '데이터', '타입']) or \
             cl_nospace in ('datatype', 'type', 'format'):
            col_map.setdefault('type', i)
        elif cl == 'type' or cl == 'format':
            col_map.setdefault('type', i)
        elif any(k in cl for k in ['unit', '단위']) or cl_nospace == 'unit':
            col_map.setdefault('unit', i)
        elif any(k in cl for k in ['scale', '배율', 'factor', 'gain']) or cl_nospace == 'gain':
            col_map.setdefault('scale', i)
        elif any(k in cl for k in ['r/w', 'access', '읽기', 'permission', '속성']) or \
             cl_nospace in ('readandwrite', 'r/w', 'readwrite'):
            col_map.setdefault('rw', i)
        elif any(k in cl for k in ['remark', 'comment', '비고', 'note', '설명', 'scope']):
            col_map.setdefault('comment', i)
        elif ('register' in cl and ('number' in cl or 'num' in cl or 'count' in cl)) or \
             cl_nospace in ('numberofregister', 'numberofreg', 'regs', 'regcount', 'numberofreg'):
            col_map.setdefault('regs', i)

    if data_rows and 'addr' not in col_map:
        # 1순위: 0x 패턴
        for row in data_rows[:5]:
            for i, cell in enumerate(row):
                if cell and re.match(r'0x[0-9A-Fa-f]{4}', str(cell).strip()):
                    col_map['addr'] = i
                    break
            if 'addr' in col_map:
                break

        # 2순위: 4~5자리 decimal (Sungrow 5000+ 등) — 행번호(1~3자리)와 구분
        if 'addr' not in col_map:
            col_scores = {}  # {col_idx: count_of_4digit_numbers}
            for row in data_rows[:10]:
                for i, cell in enumerate(row):
                    c = str(cell).strip() if cell else ''
                    if re.match(r'^\d{4,5}$', c) and 0 <= int(c) <= 65535:
                        col_scores[i] = col_scores.get(i, 0) + 1
            if col_scores:
                best_col = max(col_scores, key=col_scores.get)
                if col_scores[best_col] >= 2:
                    col_map['addr'] = best_col

    return col_map


def _parse_register_row(row: list, col_map: dict) -> Optional[RegisterRow]:
    """테이블 행 → RegisterRow"""
    if not row:
        return None

    # 원본 보존 + 줄바꿈→공백 버전 둘 다 준비
    raw_row = [str(c) if c is not None else '' for c in row]
    # 기본 row: 줄바꿈→공백 (주소/타입/단위 파싱용)
    row = [re.sub(r'\s*\n\s*', ' ', c).strip() for c in raw_row]
    _name_extra = ''  # 이름 셀에서 분리된 설명

    _RANGE_ADDR_RE = re.compile(r'^(\d{4,5})\s*[-–~]\s*(\d{4,5})$')
    _RANGE_HEX_RE = re.compile(r'^(0x[0-9A-Fa-f]{4})\s*[-–~]\s*(0x[0-9A-Fa-f]{4})$', re.I)

    addr = None
    addr_raw = ''

    def _try_parse_addr(raw: str):
        raw = raw.replace('\n', '').replace('\r', '').replace(' ', '').strip()
        if not raw:
            return None
        if raw.startswith('0x') or raw.startswith('0X'):
            return parse_address(raw)
        m = _RANGE_HEX_RE.match(raw)
        if m:
            return parse_address(m.group(1))
        if re.match(r'^\d{1,5}$', raw) and 0 <= int(raw) <= 65535:
            return int(raw)
        m = _RANGE_ADDR_RE.match(raw)
        if m:
            return int(m.group(1))
        return None

    addr_idx = col_map.get('addr')
    if addr_idx is not None and addr_idx < len(row):
        addr_raw = str(row[addr_idx]).strip() if row[addr_idx] else ''
        addr = _try_parse_addr(addr_raw)

    if addr is None:
        # V2: 4~5자리 숫자 우선 (행번호 1~3자리와 구분)
        candidates = []
        for i, cell in enumerate(row):
            c = str(cell).strip() if cell else ''
            parsed = _try_parse_addr(c)
            if parsed is not None:
                candidates.append((i, c, parsed))
        if candidates:
            # 4~5자리 decimal 우선, 없으면 0x 패턴, 최후에 아무거나
            best = None
            for i, c, parsed in candidates:
                if re.match(r'^\d{4,5}$', c):
                    best = (i, c, parsed)
                    break
            if not best:
                for i, c, parsed in candidates:
                    if c.startswith('0x') or c.startswith('0X'):
                        best = (i, c, parsed)
                        break
            if not best:
                best = candidates[0]
            addr = best[2]
            addr_raw = best[1]
            if 'addr' not in col_map:
                col_map['addr'] = best[0]

    if addr is None:
        return None

    # V2: 주소 컬럼 인덱스 (col_map 또는 fallback에서 설정된 것)
    actual_addr_idx = col_map.get('addr')

    name_idx = col_map.get('name')
    if name_idx is not None and name_idx < len(row):
        name = str(row[name_idx]).strip()
    else:
        name = ''
        for i, cell in enumerate(row):
            if i == actual_addr_idx:
                continue
            c = str(cell).strip()
            if not c or len(c) <= 2:
                continue
            # V2: 숫자만 있는 셀은 행번호 — 이름이 아님
            if re.match(r'^\d{1,5}$', c):
                continue
            # 0x 주소 패턴도 건너뛰기
            if _ADDR_RE.fullmatch(c):
                continue
            name = c
            break
    if not name:
        return None

    # 이름에 설명이 합쳐진 경우: 원본 셀에서 줄바꿈 기준 첫 줄만 이름으로 사용
    # (name_col 감지 실패 시 fallback 경로에서도 적용)
    if not _name_extra:
        # 원본 raw_row에서 name에 해당하는 셀 찾기
        for ci, c in enumerate(raw_row):
            if c and name in re.sub(r'\s*\n\s*', ' ', c) and '\n' in c:
                lines = c.strip().split('\n')
                first_line = lines[0].strip()
                if len(first_line) >= 3 and first_line != name:
                    _name_extra = ' '.join(l.strip() for l in lines[1:] if l.strip())
                    name = first_line
                break

    dtype = ''
    type_idx = col_map.get('type')
    if type_idx is not None and type_idx < len(row):
        # 'ST R' → 'STR' (PDF 줄바꿈으로 공백 삽입)
        type_cell = str(row[type_idx]).replace(' ', '')
        m = _TYPE_RE.search(type_cell)
        if m:
            dtype = m.group(1).upper()
    if not dtype:
        for cell in row:
            m = _TYPE_RE.search(str(cell).replace(' ', ''))
            if m:
                dtype = m.group(1).upper()
                break
    dtype = dtype.upper()
    for old, new in [('INT16', 'S16'), ('UINT16', 'U16'), ('INT32', 'S32'), ('UINT32', 'U32'),
                     ('I16', 'S16'), ('I32', 'S32'), ('US16', 'U16'), ('US32', 'U32'),
                     ('STR', 'STRING'), ('BITFIELD16', 'U16'), ('BITFIELD32', 'U32')]:
        dtype = dtype.replace(old, new)

    # 단위 + 스케일 추출 — "0.1V", "0.01A", "kWh" 등에서 분리
    _SCALE_UNIT_RE = re.compile(r'^([\d.]+)\s*(V|A|W|kW|KW|VA|kVA|KVA|VAr|kVAr|KVar|Hz|°C|℃|Wh|kWh|KWh|Kwh|KWH|MWh|MWH|%)$')

    unit = ''
    scale = ''

    # 1) scale 컬럼에서 먼저 시도
    scale_idx = col_map.get('scale')
    if scale_idx is not None and scale_idx < len(row):
        s = str(row[scale_idx]).strip()
        if s and s not in ('', 'None', '-'):
            # "0.1V" 형태면 scale+unit 분리
            m = _SCALE_UNIT_RE.match(s)
            if m:
                scale = m.group(1)
                unit = m.group(2)
            else:
                # 숫자만이면 scale
                try:
                    float(s)
                    scale = s
                except ValueError:
                    pass

    # 2) unit 컬럼에서 시도
    unit_idx = col_map.get('unit')
    if unit_idx is not None and unit_idx < len(row):
        u = str(row[unit_idx]).strip()
        if u and u not in ('', 'None', '-'):
            # "0.1V", "0.01A" 형태면 분리
            m = _SCALE_UNIT_RE.match(u)
            if m:
                if not scale:
                    scale = m.group(1)
                if not unit:
                    unit = m.group(2)
            else:
                # 순수 단위
                m2 = _UNIT_RE.search(u)
                if m2 and not unit:
                    unit = m2.group(1)

    # 3) 이름에서 단위 추출 (fallback)
    if not unit:
        m = _UNIT_RE.search(name)
        if m:
            unit = m.group(1)

    # 4) 전체 행에서 "0.1V", "0.01A" 패턴만 탐색 (addr/name 컬럼 제외)
    if not scale:
        skip_cols = {col_map.get('addr'), col_map.get('name'), actual_addr_idx}
        for ci, cell in enumerate(row):
            if ci in skip_cols:
                continue
            c = str(cell).strip()
            m = _SCALE_UNIT_RE.match(c)
            if m and float(m.group(1)) != 1.0:
                scale = m.group(1)
                if not unit:
                    unit = m.group(2)
                break

    rw = ''
    rw_idx = col_map.get('rw')
    if rw_idx is not None and rw_idx < len(row):
        m = _RW_RE.search(str(row[rw_idx]))
        if m:
            rw = m.group(1).upper().replace('READ', 'RO').replace('WRITE', 'WO').replace('RD', 'RO').replace('WR', 'WO')
    if not rw:
        for cell in row:
            m = _RW_RE.search(str(cell))
            if m:
                rw = m.group(1).upper().replace('READ', 'RO').replace('WRITE', 'WO').replace('RD', 'RO').replace('WR', 'WO')
                break

    comment = ''
    comment_idx = col_map.get('comment')
    if comment_idx is not None and comment_idx < len(row):
        comment = str(row[comment_idx]).strip()
        if comment in ('None', '-'):
            comment = ''
    # 이름 셀에서 분리된 설명을 comment에 추가
    if _name_extra:
        comment = f'{_name_extra} | {comment}'.strip(' |') if comment else _name_extra

    regs_val = '1'
    regs_idx = col_map.get('regs')
    if regs_idx is not None and regs_idx < len(row):
        r = str(row[regs_idx]).strip()
        if r.isdigit():
            regs_val = r
    if dtype in ('U32', 'S32', 'FLOAT32'):
        regs_val = '2'
    if dtype == 'ASCII':
        regs_val = str(max(1, int(regs_val)))

    return RegisterRow(
        definition=name,
        address=addr,
        address_hex=f'0x{addr:04X}',
        data_type=dtype or 'U16',
        regs=regs_val,
        unit=unit,
        scale=scale,
        rw=rw or 'RO',
        comment=comment,
    )


def _clean_cell(cell) -> str:
    if cell is None:
        return ''
    return re.sub(r'\s*\n\s*', '', str(cell)).strip()


def _clean_table(table: List[list]) -> List[list]:
    return [[_clean_cell(c) for c in row] for row in table]


# V2: 유효하지 않은 레지스터 이름 필터
_JUNK_NAME_RE = re.compile(r'^\d{1,5}$')  # 숫자만 (105, 220 등)
_MODEL_NAME_RE = re.compile(
    r'^SG\d+|^SH\d+|^SC\d+|^KSG|^SUN\d+|^HU?N?\d+|^GW\d+|'  # Sungrow/Huawei/Goodwe 모델명
    r'^[A-Z]{2,5}\d{2,}[A-Z]*[-_]',  # 일반 모델명 패턴 (SG60KTL-M 등)
    re.I
)
_COUNTRY_NAME_RE = re.compile(
    r'^(?:Mexico|Brazil|Germany|France|Italy|Spain|Australia|India|China|Japan|Korea|'
    r'Thailand|Vietnam|Philippines|South Africa|UK|USA|Canada|Turkey|Holland|'
    r'Belgium|Austria|Denmark|Sweden|Norway|Finland|Poland|Czech|Hungary|Romania|'
    r'Portugal|Greece|Israel|Chile|Colombia|Peru|Argentina|Egypt|Jordan|Saudi|'
    r'UAE|Kuwait|Taiwan|Malaysia|Indonesia|Singapore|New Zealand|'
    r'Oman|Ireland|America|Vorarlberg|AU-WEST|Other\s+\d+Hz|GR\s+IS)',
    re.I
)


def _is_valid_register_name(name: str) -> bool:
    """V2: 유효한 레지스터 이름인지 판단"""
    stripped = name.strip()
    if not stripped or len(stripped) < 2:
        return False
    # 숫자만 (PDF 값이 이름으로 추출됨)
    if _JUNK_NAME_RE.match(stripped):
        return False
    # 인버터 모델명 테이블 (SG60KTL, SG50KTL-M 등)
    if _MODEL_NAME_RE.match(stripped):
        return False
    # 국가명/국가코드 테이블
    if _COUNTRY_NAME_RE.match(stripped):
        return False
    # 숫자 + 짧은 단위만 (예: "220V", "50Hz" — 설정값)
    if re.match(r'^\d+\.?\d*\s*[A-Za-z%°]{0,3}$', stripped):
        return False
    # V2: 무의미한 이름 (Reserved, U16 등 — 데이터 타입이 이름으로 추출된 경우)
    stripped_lower = stripped.lower()
    if stripped_lower in ('reserved', 'u16', 'u32', 's16', 's32', 'n/a', 'none', '-', '--'):
        return False
    # V2: Q(P)/Q(U) 커브 파라미터 (QP P1_, QU V1_, Q U1_, Curve 등)
    if re.match(r'^Q[PU ]\s*[A-Z]\d', stripped, re.I):
        return False
    if re.match(r'^LP\s+P\d', stripped, re.I):  # LP P34KSG_ 등
        return False
    # V2: 상태값 테이블 엔트리 (Initial standby, Starting, Stop, Derating run 등)
    if stripped_lower in ('initial standby', 'standby', 'starting', 'stop',
                           'derating run', 'dispatch run', 'key stop',
                           'curve', 'device abnormal'):
        return False
    # V2: 너무 짧은 이름 (3자 이하인데 키워드가 아닌 것)
    if len(stripped) <= 3 and not any(k in stripped_lower for k in
            ['sn', 'pn', 'pf', 'pv', 'dc', 'ac', 'bus', 'ia', 'ib', 'ic', 'ua', 'ub', 'uc',
             '모델', '버전', '용량', '전압', '전류', '전력', '온도', '상태']):
        return False
    return True


def extract_registers_from_tables(tables: List[List[list]], fc_list: list = None) -> List[RegisterRow]:
    """모든 테이블에서 레지스터 행 추출. fc_list가 있으면 각 테이블의 FC(03/04) 태깅."""
    registers = []
    seen_addr_fc = set()  # (addr, fc) 튜플로 중복 체크 — FC 구분 지원
    prev_col_map = {}
    for ti, table in enumerate(tables):
        if not table or len(table) < 1:
            continue
        fc = fc_list[ti] if fc_list and ti < len(fc_list) else ''
        table = _clean_table(table)
        first_has_addr = any(
            str(c).strip().startswith('0x') or str(c).strip().startswith('0X') or
            (re.match(r'^\d{3,5}$', str(c).strip()) and 0 <= int(str(c).strip()) <= 65535)
            for c in table[0] if c)
        if first_has_addr:
            data_rows = table
            col_map = _detect_table_columns([], data_rows)
            if prev_col_map and len(col_map) < len(prev_col_map):
                col_map = dict(prev_col_map)
        else:
            data_rows = table[1:]
            col_map = _detect_table_columns(table[0], data_rows)
            if col_map:
                prev_col_map = dict(col_map)
        for row in data_rows:
            reg = _parse_register_row(row, col_map)
            if reg:
                dedup_key = (reg.address, fc)
                if dedup_key in seen_addr_fc:
                    continue
                if not _is_valid_register_name(reg.definition):
                    continue
                reg.fc = fc
                seen_addr_fc.add(dedup_key)
                registers.append(reg)
    return registers


# ─── H01 필드 매핑 ──────────────────────────────────────────────────────────

# ─── 정의 테이블 탐색 (Definition-Based Matching) ────────────────────────

_DEF_TABLE_HEADER_RE = re.compile(
    r'(?:Error\s*Code\s*Table\s*(\d)?|Alarm\s*(\d)|Fault\s*Code|'
    r'Inverter\s*Mode\s*Table|Work\s*(?:Mode|State)\s*Table|'
    r'Running\s*Status|Operating\s*Mode|Device\s*Status|'
    r'Table\s*3\.1\.\d|DSP\s*(?:alarm|error)|ARM\s*alarm)\s*'
    r'(?:\(?(?:0x([0-9A-Fa-f]{4}))\)?)?',
    re.I
)
_BIT_DEF_RE = re.compile(r'^(?:Bit\s*)?(\d{1,2})$')
_HEX_VAL_RE = re.compile(r'^0x([0-9A-Fa-f]{2,4})$')
_MODE_VAL_RE = re.compile(r'^(\d{1,2})\s*[:：]\s*(.+)')


def scan_definition_tables(pages: list, excel_sheets: dict = None,
                           registers: list = None) -> dict:
    """
    PDF/Excel에서 STATUS/ALARM 정의 테이블 탐색
    3가지 경로:
    1. Appendix/본문 페이지에서 정의 테이블 탐색
    2. Excel 시트(EKOS Fault MAP, 데이타형식)에서 탐색
    3. 레지스터 comment에서 인라인 정의 추출
    """
    result = {'status_defs': [], 'alarm_defs': []}

    if pages:
        _scan_pdf_definitions(pages, result)
    if excel_sheets:
        _scan_excel_definitions(excel_sheets, result)
    if registers:
        _scan_register_comments(registers, result)

    return result


def _scan_register_comments(registers: list, result: dict):
    """레지스터 이름/comment에서 인라인 정의 추출"""
    for reg in registers:
        comment = (getattr(reg, 'comment', '') or '').strip()
        defn = (reg.definition or '').strip()
        addr = reg.address if isinstance(reg.address, int) else 0
        category = getattr(reg, 'category', '')

        if category not in ('STATUS', 'ALARM') and not any(
                k in defn.lower() for k in ['mode', 'status', 'state', 'error', 'alarm', 'fault']):
            continue

        combined = f'{defn} {comment}'.lower()

        # 1) comment에 인라인 값 정의: "0:cWaitMode1:cNormalMode2:cFaultMode"
        inline_vals = {}
        for m in re.finditer(r'(\d+)\s*[:：]\s*([a-zA-Z가-힣]\w{2,})', comment):
            inline_vals[int(m.group(1))] = m.group(2)
        if len(inline_vals) >= 2:
            def_type = _detect_definition_type(inline_vals)
            if def_type:
                target = 'status_defs' if def_type == 'mode_table' else 'alarm_defs'
                result[target].append({
                    'address': addr, 'name': f'{defn} (comment)',
                    'type': def_type, 'values': inline_vals, 'page': -1,
                })
                continue

        # 2) comment에 테이블 참조: "Table 3.1.4", "See Appendix 3", "Error Code Table 1"
        has_table_ref = bool(re.search(r'Table\s+[\d.]+|Appendix\s+\d|Code\s+Table', comment, re.I))
        if has_table_ref:
            # 정의는 Appendix에서 이미 탐색했으므로, 레지스터에 참조 마크만
            # 나중에 run_stage1에서 연결
            pass

        # 3) 레지스터 이름에 "Mode Table", "Error Code" 참조
        if any(k in combined for k in ['mode table', 'error code table', 'fault code table',
                                        'alarm code table']):
            # 이미 Appendix 탐색에서 찾았을 가능성 — 주소 연결용
            pass


def _scan_pdf_definitions(pages: list, result: dict):
    """PDF 페이지에서 STATUS/ALARM 정의 테이블 탐색 (정밀 버전)"""
    for p in pages:
        text = p.get('text', '')
        page_num = p.get('page', 0)
        lines = text.split('\n')

        # ── 1) 헤더 기반: "Error Code Table1 (0x101E)" 등 ──
        for line in lines:
            line_s = line.strip()
            m = _DEF_TABLE_HEADER_RE.search(line_s)
            if not m:
                continue
            addr_hex = m.group(3)
            addr = int(addr_hex, 16) if addr_hex else None
            header_lower = line_s.lower()
            is_mode = any(k in header_lower for k in ['mode', 'status', 'state', 'running', 'operating'])
            is_alarm = any(k in header_lower for k in ['error code', 'alarm', 'fault', 'dsp', 'arm'])

            for tab in p.get('tables', []):
                cleaned = _clean_table(tab)
                values = _extract_value_definitions(cleaned)
                if values and len(values) >= 2:
                    def_type = _detect_definition_type(values)
                    if def_type:
                        target = 'status_defs' if (is_mode or def_type == 'mode_table') else 'alarm_defs'
                        result[target].append({
                            'address': addr, 'name': line_s[:60],
                            'type': def_type, 'values': values, 'page': page_num,
                        })
                        break

        # ── 2) 테이블 기반: hex 모드값 테이블 (Solarize Table: 0x00=Initial) ──
        for tab in p.get('tables', []):
            cleaned = _clean_table(tab)
            hex_modes = {}
            for row in cleaned:
                cells = [str(c).strip() for c in row]
                if not cells:
                    continue
                # "0x00 | (empty) | Initial mode" 또는 "0x00 | Initial mode"
                m_hex = _HEX_VAL_RE.match(cells[0])
                if m_hex:
                    val = int(m_hex.group(1), 16)
                    # 설명은 비어있지 않은 셀에서
                    desc = ''
                    for c in cells[1:]:
                        if c and len(c) > 2 and not c.isdigit() and not c.startswith('0x'):
                            desc = c[:50]
                            break
                    dl = desc.lower()
                    if any(k in dl for k in ['mode', 'standby', 'on-grid', 'fault', 'shutdown',
                                              'initial', 'off-grid', 'waiting', 'normal']):
                        hex_modes[val] = desc
            if len(hex_modes) >= 3:
                if not any(d['page'] == page_num and d['type'] == 'mode_table'
                           and len(d['values']) >= 3 for d in result['status_defs']):
                    result['status_defs'].append({
                        'address': None, 'name': f'Mode Table (P{page_num})',
                        'type': 'mode_table', 'values': hex_modes, 'page': page_num,
                    })

        # ── 2b) 텍스트 기반: hex 상태값 (Huawei: "0x0000 Standby: initializing") ──
        hex_status = {}
        for line in lines:
            m = re.match(r'\s*(0x[0-9A-Fa-f]{2,4})\s+(.+)', line.strip())
            if m:
                val = int(m.group(1), 16)
                desc = m.group(2).strip()[:50]
                dl = desc.lower()
                if any(k in dl for k in ['standby', 'fault', 'shutdown', 'running',
                                          'on-grid', 'off-grid', 'detecting', 'spot']):
                    hex_status[val] = desc
        if len(hex_status) >= 3:
            if not any(d['page'] == page_num and 'Hex' in d.get('name', '')
                       for d in result['status_defs']):
                result['status_defs'].append({
                    'address': None, 'name': f'Hex Status (P{page_num})',
                    'type': 'mode_table', 'values': hex_status, 'page': page_num,
                })

        # ── 3) 텍스트 기반: "N: description" 패턴 ──
        # Huawei P158: "0: offline", "1: standby", "3: faulty", "4: running"
        # Goodwe: "0:cWaitMode", "1:cNormalMode", "2:cFaultMode"
        num_status = {}
        for line in lines:
            m = re.match(r'\s*(\d{1,2})\s*[:：]\s*(.+)', line.strip())
            if m:
                val = int(m.group(1))
                desc = m.group(2).strip()[:50]
                dl = desc.lower()
                if any(k in dl for k in ['offline', 'standby', 'faulty', 'running', 'power',
                                          'mode', 'wait', 'normal', 'fault', 'stop',
                                          'initializ', 'check', 'mpp', 'search', 'close']):
                    num_status[val] = desc
        if len(num_status) >= 3:
            if not any(d['page'] == page_num for d in result['status_defs']):
                result['status_defs'].append({
                    'address': None, 'name': f'Num Status (P{page_num})',
                    'type': 'mode_table', 'values': num_status, 'page': page_num,
                })

        # ── 3b) 테이블 기반: Sungrow "State | Value(0x hex) | Paraphrase" ──
        for tab in p.get('tables', []):
            cleaned = _clean_table(tab)
            if len(cleaned) < 4:
                continue
            # 헤더에 "State" + "Value" 패턴
            header_joined = ' '.join(str(c).lower() for c in cleaned[0] + (cleaned[1] if len(cleaned) > 1 else []))
            if 'state' in header_joined and 'value' in header_joined:
                sg_modes = {}
                for row in cleaned[2:]:
                    cells = [str(c).strip() for c in row]
                    if len(cells) >= 2 and cells[0] and cells[1]:
                        state_name = cells[0]
                        value_str = cells[1]
                        # "0x0" "0x8000" "0x1300" 패턴
                        m_v = re.match(r'^0x([0-9A-Fa-f]+)$', value_str)
                        if m_v:
                            val = int(m_v.group(1), 16)
                            sg_modes[val] = state_name
                if len(sg_modes) >= 4:
                    result['status_defs'].append({
                        'address': None, 'name': f'Work State Table (P{page_num})',
                        'type': 'mode_table', 'values': sg_modes, 'page': page_num,
                    })

        # ── 3c) 테이블 기반: Kstar "SN | Content(00H) | Description" ──
        for tab in p.get('tables', []):
            cleaned = _clean_table(tab)
            if len(cleaned) < 4:
                continue
            header_joined = ' '.join(str(c).lower() for c in cleaned[0])
            if ('sn' in header_joined or 'content' in header_joined) and 'description' in header_joined:
                ks_modes = {}
                for row in cleaned[1:]:
                    cells = [str(c).strip() for c in row]
                    if len(cells) >= 3 and cells[0].isdigit():
                        val = int(cells[0])
                        desc = cells[2] if cells[2] else cells[1]
                        dl = desc.lower()
                        if any(k in dl for k in ['initialization', 'waiting', 'normal', 'error',
                                                  'permanent', 'aging', 'burning', 'detection',
                                                  'checking', 'standby']):
                            ks_modes[val] = desc
                if len(ks_modes) >= 3:
                    result['status_defs'].append({
                        'address': None, 'name': f'Operating Mode (P{page_num})',
                        'type': 'mode_table', 'values': ks_modes, 'page': page_num,
                    })

        # ── 4) 테이블 기반: Sungrow Appendix fault codes ──
        # "002 | 0x0002 | Grid overvoltage | Fault"
        for tab in p.get('tables', []):
            cleaned = _clean_table(tab)
            fault_codes = {}
            for row in cleaned:
                cells = [str(c).strip() for c in row]
                if len(cells) >= 3:
                    # 숫자(코드) + 0x hex + 설명 + 분류(Fault/Alarm/Warning)
                    c0, c1 = cells[0], cells[1]
                    if c0.isdigit() and int(c0) > 0 and c1.startswith('0x'):
                        desc = cells[2][:50] if len(cells) > 2 else ''
                        classification = cells[3].lower() if len(cells) > 3 else ''
                        if any(k in classification for k in ['fault', 'alarm', 'warning', 'protect']):
                            fault_codes[int(c0)] = f'{desc} ({cells[3]})' if len(cells) > 3 else desc
            if len(fault_codes) >= 5:
                if not any(d['page'] == page_num and d['type'] == 'fault_codes'
                           for d in result['alarm_defs']):
                    result['alarm_defs'].append({
                        'address': None, 'name': f'Fault Codes (P{page_num})',
                        'type': 'fault_codes', 'values': fault_codes, 'page': page_num,
                    })

        # ── 5) 테이블 기반: Kstar/Huawei bitfield ──
        # "0 | Bit0 | F00 | Grid Volt Low" — Bit{N} 키워드가 있는 테이블만
        for tab in p.get('tables', []):
            cleaned = _clean_table(tab)
            # 테이블에 "Bit" 키워드가 있는지 먼저 확인
            has_bit_keyword = any('Bit' in str(c) or 'bit' in str(c)
                                  for row in cleaned[:3] for c in row)
            if not has_bit_keyword:
                continue
            bits = {}
            for row in cleaned:
                cells = [str(c).strip() for c in row]
                if len(cells) >= 2:
                    # "0 | Bit0 | W00 | Fan A Lock" 또는 "Bit0 | F00 | Grid Volt Low"
                    for ci, c in enumerate(cells[:3]):
                        m_bit = re.match(r'^Bit\s*(\d{1,2})$', c)
                        if m_bit:
                            bit_num = int(m_bit.group(1))
                            desc = ''
                            for dc in reversed(cells[ci+1:]):
                                if dc and len(dc) > 2 and not dc.isdigit() and not dc.startswith('Bit'):
                                    desc = dc[:50]
                                    break
                            if desc and 0 <= bit_num <= 31:
                                bits[bit_num] = desc
                            break
            if len(bits) >= 3:
                if not any(d['page'] == page_num and d['type'] == 'bitfield'
                           for d in result['alarm_defs']):
                    result['alarm_defs'].append({
                        'address': None, 'name': f'Bitfield (P{page_num})',
                        'type': 'bitfield', 'values': bits, 'page': page_num,
                    })

        # ── 6) Huawei 텍스트 기반 bitfield ──
        # "Bit00: input overvoltage", "Bit01: input undervoltage"
        bit_defs = {}
        for line in lines:
            m = re.match(r'\s*Bit\s*(\d{1,2})\s*[:：]\s*(.+)', line.strip())
            if m:
                bit_num = int(m.group(1))
                desc = m.group(2).strip()[:50]
                if desc and 0 <= bit_num <= 31:
                    bit_defs[bit_num] = desc
        if len(bit_defs) >= 3:
            if not any(d['page'] == page_num and d['type'] == 'bitfield'
                       for d in result['alarm_defs']):
                result['alarm_defs'].append({
                    'address': None, 'name': f'Bit Defs (P{page_num})',
                    'type': 'bitfield', 'values': bit_defs, 'page': page_num,
                })


def _scan_excel_definitions(sheets: dict, result: dict):
    """Excel 시트에서 정의 테이블 탐색 (EKOS 등)"""
    for key, rows in sheets.items():
        key_lower = key.lower()

        # EKOS: 데이타형식 F007 → status 정의
        if '데이타' in key_lower or 'format' in key_lower:
            in_f007 = False
            values = {}
            for row in rows:
                joined = ' '.join(str(c) for c in row).lower()
                if 'f007' in joined or '인터터 동작상태' in joined or '인버터 동작상태' in joined:
                    in_f007 = True
                    continue
                if in_f007:
                    if 'f008' in joined or 'f009' in joined:
                        break
                    # "0 : Stop", "8 : MPP" 패턴
                    for cell in row:
                        c = str(cell).strip()
                        m = re.match(r'^(\d+)\s*[:：]\s*(.+)', c)
                        if m:
                            values[int(m.group(1))] = m.group(2).strip()
            if len(values) >= 2:
                result['status_defs'].append({
                    'address': None, 'name': 'F007 (EKOS)',
                    'type': 'mode_table', 'values': values, 'page': 0,
                })

        # EKOS: Fault MAP → alarm 정의
        if 'fault' in key_lower:
            current_group = ''
            values = {}
            for row in rows:
                cells = [str(c).strip() if c else '' for c in row]
                # 그룹 헤더 (PV, 인버터, 계통, 컨버터)
                if cells[0] and any(k in cells[0] for k in ['PV', '인버터', '계통', '컨버터']):
                    if values and current_group:
                        result['alarm_defs'].append({
                            'address': None, 'name': f'{current_group} (EKOS)',
                            'type': 'bitfield', 'values': dict(values), 'page': 0,
                        })
                    current_group = cells[0].replace('\n', ' ').strip()[:30]
                    values = {}
                    continue
                # 비트 정의: bit번호 + 설명
                if len(cells) >= 3 and cells[1].isdigit() and cells[2]:
                    bit_num = int(cells[1])
                    desc = cells[2]
                    if desc:
                        values[bit_num] = desc
            if values and current_group:
                result['alarm_defs'].append({
                    'address': None, 'name': f'{current_group} (EKOS)',
                    'type': 'bitfield', 'values': dict(values), 'page': 0,
                })


def _extract_value_definitions(table: list) -> dict:
    """테이블에서 값 → 설명 매핑 추출 (레지스터 데이터 테이블 제외)"""
    values = {}
    for row in table:
        cells = [str(c).strip() for c in row if str(c).strip()]
        if not cells or len(cells) < 2:
            continue
        # 레지스터 데이터 테이블 행 제외 (U16, U32, S16, RO, RW 등 포함)
        joined = ' '.join(cells)
        if re.search(r'\b(U16|U32|S16|S32|FLOAT32|RO|RW|04H|06H)\b', joined):
            continue
        # Bit N 패턴
        m_bit = _BIT_DEF_RE.match(cells[0])
        if m_bit:
            bit_num = int(m_bit.group(1))
            desc = cells[-1] if len(cells) > 1 else ''
            if desc and len(desc) > 2 and not desc.isdigit():
                values[bit_num] = desc[:50]
            continue
        # 0x hex 값 패턴
        m_hex = _HEX_VAL_RE.match(cells[0])
        if m_hex:
            val = int(m_hex.group(1), 16)
            desc = cells[1] if len(cells) > 1 else ''
            if desc and not desc.isdigit():
                values[val] = desc[:50]
            continue
        # 숫자 + 설명
        if cells[0].isdigit() and len(cells) >= 2:
            val = int(cells[0])
            desc = cells[1]
            if desc and len(desc) > 2 and not desc.isdigit():
                values[val] = desc[:50]
    return values


def _detect_definition_type(values: dict) -> Optional[str]:
    """값 패턴으로 정의 타입 추측"""
    if not values:
        return None
    keys = list(values.keys())
    vals_text = ' '.join(str(v).lower() for v in values.values())

    # mode_table: 값이 적고 (< 15), mode/standby/fault 키워드
    if len(keys) <= 15 and any(k in vals_text for k in ['mode', 'standby', 'fault', 'grid',
                                                          'initial', 'shutdown', 'waiting',
                                                          'normal', 'running', 'stop']):
        return 'mode_table'

    # bitfield: 키가 0~15 범위, bit 관련 키워드
    if all(0 <= k <= 31 for k in keys) and any(k in vals_text for k in [
            'over', 'under', 'abnormal', 'fault', 'lock', 'relay', 'igbt',
            'voltage', 'current', 'frequency', 'temperature']):
        return 'bitfield'

    # fault_codes: 키가 크고 (> 15), fault/alarm 분류
    if any(k > 15 for k in keys) and any(k in vals_text for k in ['fault', 'alarm', 'warning']):
        return 'fault_codes'

    return None


def assign_h01_field(reg: RegisterRow, synonym_db: dict,
                     ref_patterns: dict = None) -> str:
    """레지스터에 대응하는 H01 필드명 추정 (V2)"""
    defn_lower = reg.definition.lower().replace('_', ' ')
    category = getattr(reg, 'category', '')

    # V2: INFO/ALARM 카테고리는 H01 모니터링 필드가 아님 — 특정 키워드만 매칭
    comment_lower = (getattr(reg, 'comment', '') or '').lower()

    if category == 'INFO':
        # INFO에서 H01과 겹치는 필드만 매핑
        # cumulative_energy LOW (소수부/Wh): decimal, low byte — comment도 체크
        if any(k in defn_lower for k in ['decimals of total energy', 'decimal of total',
                                          'low byte of total feed', 'low byte oftotal feed']) or \
           any(k in comment_lower for k in ['decimals of total', 'decimal of total']):
            return 'cumulative_energy_low'
        # cumulative_energy HIGH (정수부)
        if any(k in defn_lower for k in ['total energy', 'cumulative energy', 'total power yields',
                                          'total energy yield', 'energy yield',
                                          '누적발전량', '누적 발전량',
                                          'total generation energy',
                                          'high byte of total feed', 'high byte oftotal feed']):
            return 'cumulative_energy'
        # 적산전력량 MWh/Wh 쌍 (EKOS)
        if '적산전력량' in defn_lower:
            unit = getattr(reg, 'unit', '').upper()
            if unit in ('WH', 'wh', 'Wh'):
                return 'cumulative_energy_low'
            return 'cumulative_energy'
        if any(k in defn_lower for k in ['daily energy', 'today energy', 'daily power yields',
                                          '일발전량', '일 발전량', '금일발전량']):
            return 'daily_energy'
        if any(k in defn_lower for k in ['inner_temp', 'inner temp', 'internal temp',
                                          'module temp', 'inverter temp']):
            return 'temperature'
        return ''  # 나머지 INFO는 h01_field 없음

    # V2: STATUS 카테고리 — inverter_status 매핑
    if category == 'STATUS':
        if any(k in defn_lower for k in ['inverter mode', 'work mode', 'work state',
                                          'operating mode', 'running status', 'run mode',
                                          '인버터 모드', '시스템동작상태', '동작상태',
                                          'device status', 'system status']):
            return 'inverter_status'
        return ''

    # 0) 채널 번호가 있으면 최우선
    ch = detect_channel_number(reg.definition)
    if ch:
        prefix, n = ch
        if prefix == 'MPPT':
            if 'voltage' in defn_lower or '전압' in defn_lower:
                return f'mppt{n}_voltage'
            if 'current' in defn_lower or '전류' in defn_lower:
                return f'mppt{n}_current'
            if 'power' in defn_lower or '전력' in defn_lower:
                return f'mppt{n}_power'
        elif prefix == 'STRING':
            if 'voltage' in defn_lower or '전압' in defn_lower:
                return f'string{n}_voltage'
            if 'current' in defn_lower or '전류' in defn_lower:
                return f'string{n}_current'

    # 1) V2: pv_power / energy 키워드 (synonym/ref보다 먼저 — 정확한 키워드 우선)
    if any(k in defn_lower for k in ['total dc power', 'total pv power', 'dc power',
                                      'pv total power', 'pv_total_input_power',
                                      'input power', 'output power',
                                      'inverter current output',
                                      'totaldc input', 'totaldcinput',
                                      '태양전지 전력', '태양전지전력']) or \
       re.search(r'\bpac\b', defn_lower):
        return 'pv_power'
    defn_nospace = defn_lower.replace(' ', '')
    # cumulative_energy LOW (소수부/Wh/Low Byte) — comment도 체크
    if any(k in defn_lower for k in ['decimals of total energy', 'decimal of total',
                                      'low byte of total feed', 'low byte oftotal feed']) or \
       any(k in comment_lower for k in ['decimals of total', 'decimal of total']):
        return 'cumulative_energy_low'
    # 적산전력량 Wh (EKOS LOW)
    if '적산전력량' in defn_lower:
        unit = getattr(reg, 'unit', '').upper()
        if unit in ('WH',):
            return 'cumulative_energy_low'
        return 'cumulative_energy'
    # cumulative_energy HIGH (정수부)
    if any(k in defn_lower for k in ['total energy', 'cumulative energy', 'total power yields',
                                      'lifetime energy', 'accumulated energy',
                                      'total power generation', 'total powergeneration',
                                      'total energy yield', 'energy yield',
                                      '누적발전량', '누적 발전량',
                                      'total generation energy',
                                      'high byte of total feed', 'high byte oftotal feed']) or \
       any(k in defn_nospace for k in ['accumulatedpower', 'accumulatedenergy',
                                        'totalpowergeneration', 'totalgenerationenergy',
                                        'totalenergyyield']):
        return 'cumulative_energy'
    if any(k in defn_lower for k in ['daily energy', 'today energy', 'daily power yields',
                                      'daily generation', '일발전량', '일 발전량',
                                      '금일발전량', '금일 발전량']):
        return 'daily_energy'

    # 2) synonym_db 정확 매칭
    syn_match = match_synonym(reg.definition, synonym_db)
    if syn_match and syn_match.get('h01_field'):
        return syn_match['h01_field']

    # 3) 주소 기반 레퍼런스 (같은 제조사만)
    if ref_patterns:
        addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)
        if addr is not None:
            h01 = get_h01_field_from_ref(addr, ref_patterns, synonym_db)
            if h01:
                return h01

    # 4) 퍼지 매칭
    fuzzy = match_synonym_fuzzy(reg.definition, synonym_db)
    if fuzzy and fuzzy.get('h01_field'):
        return fuzzy['h01_field']

    return ''


# ─── Appendix 테이블 추출 (INFO 레지스터에서 참조) ────────────────────────

_APPENDIX_REF_RE = re.compile(r'(?:See\s+)?Appendix\s+(\w+)', re.I)


def scan_info_appendix_tables(info_regs: list, pages: list) -> dict:
    """INFO 레지스터 comment/description에서 Appendix 참조를 찾아 해당 테이블 추출.

    Returns: {appendix_id: {'title': str, 'header': [...], 'rows': [[...]], 'register': str}}
    """
    if not pages:
        return {}

    # 1) INFO 레지스터에서 Appendix 참조 수집
    appendix_refs = {}  # appendix_id -> register definition
    for reg in info_regs:
        comment = getattr(reg, 'comment', '') or ''
        defn = getattr(reg, 'definition', '') or ''
        for text in [comment, defn]:
            m = _APPENDIX_REF_RE.search(text)
            if m:
                app_id = m.group(1)
                appendix_refs[app_id] = reg.definition
    if not appendix_refs:
        return {}

    # 2) PDF 페이지에서 Appendix N 테이블 찾기
    result = {}
    for app_id, reg_defn in appendix_refs.items():
        pattern = re.compile(rf'Appendix\s+{re.escape(app_id)}\b', re.I)
        best_table = None
        best_page = None
        for p in pages:
            if not pattern.search(p['text']):
                continue
            # 해당 페이지 테이블 중 데이터 테이블(레지스터맵 아닌 것) 찾기
            for tab in p.get('tables', []):
                if len(tab) < 2:
                    continue
                header = tab[0]
                header_str = ' '.join(str(c).lower() for c in header if c)
                # 레지스터맵 테이블 제외 (Address/Data type 컬럼이 있으면 스킵)
                if re.search(r'\baddress\b.*\bdata\s*type\b', header_str):
                    continue
                # Model/Type code 같은 device info 테이블 감지
                has_model = any('model' in str(c).lower() for c in header if c)
                has_type_code = any('type' in str(c).lower() and 'code' in str(c).lower()
                                    for c in header if c)
                has_mppt = any('mppt' in str(c).lower() for c in header if c)
                has_string = any('string' in str(c).lower() for c in header if c)
                # 최소 2개 특징 컬럼이 있어야 device info 테이블
                if sum([has_model, has_type_code, has_mppt, has_string]) >= 2:
                    if best_table is None or len(tab) > len(best_table):
                        best_table = tab
                        best_page = p['page']

        if best_table is None:
            continue

        # 연속 페이지에서 같은 테이블 이어붙이기 (헤더 없는 후속 테이블)
        header = best_table[0]
        header_count = len([c for c in header if c])
        all_rows = list(best_table[1:])
        for p in pages:
            if p['page'] <= best_page:
                continue
            if p['page'] > best_page + 3:  # 최대 3페이지까지 연속
                break
            found_continuation = False
            for tab in p.get('tables', []):
                if len(tab) < 1:
                    continue
                # 첫 행이 헤더가 아니고 컬럼 수가 비슷하면 연속 테이블
                first_str = ' '.join(str(c).lower() for c in tab[0] if c)
                col_count = len([c for c in tab[0] if c])
                if abs(col_count - header_count) <= 1 and 'model' not in first_str:
                    all_rows.extend(tab)
                    found_continuation = True
                    break
            if not found_continuation:
                break

        # 빈 행 / 모델명 없는 행 제거
        clean_rows = []
        for row in all_rows:
            cells = [str(c) if c is not None else '' for c in row]
            if not any(c.strip() for c in cells):
                continue
            clean_rows.append(cells)

        header_cells = [str(c) if c is not None else '' for c in header]
        result[app_id] = {
            'title': f'Appendix {app_id}',
            'header': header_cells,
            'rows': clean_rows,
            'register': reg_defn,
            'page': best_page,
        }

    return result


# ─── IV Scan 감지 ──────────────────────────────────────────────────────────

_IV_COMMAND_RE = re.compile(r'i-?v\s*(curve\s*)?scan|IV_CURVE_SCAN', re.I)
# Solarize 형식: Tracker N voltage, String N-M current
_TRACKER_VOLTAGE_RE = re.compile(
    r'Tracker\s*(\d+)\s*voltage|IV_TRACKER(\d+)_VOLTAGE(?:_BASE)?|TRACKER_(\d+)_VOLTAGE', re.I)
_IV_STRING_CURRENT_RE = re.compile(
    r'String\s*(\d+)-(\d+)\s*current|IV_STRING(\d+)_(\d+)_CURRENT(?:_BASE)?|'
    r'IV_STRING_(\d+)_(\d+)_CURRENT', re.I)
# Kstar 형식: PV1 Voltage Point 1, PV1 Current Point 1
_PV_VOLTAGE_POINT_RE = re.compile(r'PV(\d+)\s+Voltage\s+Point\s+(\d+)', re.I)
_PV_CURRENT_POINT_RE = re.compile(r'PV(\d+)\s+Current\s+Point\s+(\d+)', re.I)
# "occupying NNNN registers" 패턴
_IV_TOTAL_REGS_RE = re.compile(r'occupying\s+(\d+)\s+registers', re.I)


def detect_iv_from_pdf(registers: List[RegisterRow], pages: list = None) -> dict:
    """
    PDF에서 IV Scan 지원 여부 및 구조 감지
    Solarize 형식: Tracker N voltage (블록), String N-M current (블록)
    Kstar 형식: PV1 Voltage Point 1~100, PV1 Current Point 1~100 (교차)
    """
    result = {'supported': False, 'iv_command_addr': None, 'data_points': 0,
              'trackers': [], 'format': 'unknown', 'total_iv_regs': 0}

    # 1) IV Scan 명령 레지스터 (command만으로는 supported 판정 안 함)
    for reg in registers:
        if _IV_COMMAND_RE.search(reg.definition):
            addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)
            result['iv_command_addr'] = addr
            # supported는 데이터 레지스터 유무로 판단 (아래에서)
            break

    # 2-A) Solarize 형식: Tracker/String 블록
    tracker_map = {}
    for reg in registers:
        addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)
        if addr is None:
            continue

        m = _TRACKER_VOLTAGE_RE.search(reg.definition)
        if m:
            tn = int(next(g for g in m.groups() if g))
            tracker_map.setdefault(tn, {'voltage_addr': addr, 'regs': int(reg.regs or '1'), 'strings': {}})
            tracker_map[tn]['voltage_addr'] = addr
            tracker_map[tn]['regs'] = int(reg.regs or '1')
            result['supported'] = True
            continue

        m = _IV_STRING_CURRENT_RE.search(reg.definition)
        if m:
            groups = [g for g in m.groups() if g]
            if len(groups) >= 2:
                tn = int(groups[0])
                sn = int(groups[1])
            tracker_map.setdefault(tn, {'voltage_addr': None, 'regs': 0, 'strings': {}})
            tracker_map[tn]['strings'][sn] = addr
            result['supported'] = True

    if tracker_map:
        result['format'] = 'solarize'
        for tn in sorted(tracker_map):
            t = tracker_map[tn]
            if t.get('regs', 0) > 1:
                result['data_points'] = t['regs']
                break
        # String 주소 간격에서 추정
        if result['data_points'] == 0 and 1 in tracker_map:
            t1 = tracker_map[1]
            if t1['voltage_addr'] and t1['strings'].get(1):
                result['data_points'] = t1['strings'][1] - t1['voltage_addr']
        # Tracker 간 주소 간격에서 추정 (T2 - T1) / (1 + strings_per_tracker)
        if result['data_points'] == 0:
            sorted_trackers = sorted(tracker_map.keys())
            if len(sorted_trackers) >= 2:
                t1_addr = tracker_map[sorted_trackers[0]]['voltage_addr']
                t2_addr = tracker_map[sorted_trackers[1]]['voltage_addr']
                if t1_addr and t2_addr:
                    gap = t2_addr - t1_addr
                    n_blocks = 1 + len(tracker_map[sorted_trackers[0]].get('strings', {}))
                    if n_blocks == 1:
                        n_blocks = 5  # Solarize 기본: 1 tracker + 4 strings
                    result['data_points'] = gap // n_blocks
        for tn in sorted(tracker_map):
            t = tracker_map[tn]
            strings = [{'string_num': sn, 'current_addr': t['strings'][sn]}
                       for sn in sorted(t['strings'])]
            result['trackers'].append({
                'tracker_num': tn, 'voltage_addr': t['voltage_addr'], 'strings': strings,
            })
        return result

    # 2-B) Kstar 형식: PV{n} Voltage/Current Point {m}
    pv_map = {}  # {pv_num: {'voltage_addr': addr, 'max_point': n}}
    for reg in registers:
        addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)
        if addr is None:
            continue
        defn = reg.definition

        m = _PV_VOLTAGE_POINT_RE.search(defn)
        if m:
            pv_num = int(m.group(1))
            point = int(m.group(2))
            pv_map.setdefault(pv_num, {'voltage_addr': None, 'max_point': 0})
            if point == 1 or pv_map[pv_num]['voltage_addr'] is None:
                pv_map[pv_num]['voltage_addr'] = addr
            pv_map[pv_num]['max_point'] = max(pv_map[pv_num]['max_point'], point)
            result['supported'] = True
            continue

        # "occupying NNNN registers" 패턴에서 총 레지스터 수 추출
        m2 = _IV_TOTAL_REGS_RE.search(reg.comment or '')
        if m2:
            result['total_iv_regs'] = int(m2.group(1))

    if pv_map:
        result['format'] = 'kstar'
        # data_points = max_point (PV1 Voltage Point 100 → 100)
        max_point = max(pv['max_point'] for pv in pv_map.values())
        result['data_points'] = max_point

        # Tracker = PV (Kstar에서는 PV = MPPT = Tracker)
        for pv_num in sorted(pv_map):
            pv = pv_map[pv_num]
            result['trackers'].append({
                'tracker_num': pv_num,
                'voltage_addr': pv['voltage_addr'],
                'strings': [],  # Kstar는 PV 단위 (String 분리 없음)
            })
        return result

    return result


# ─── H01 매칭 상태 표 ──────────────────────────────────────────────────────

# H01 필드별 기본 단위 (RTU UDP 프로토콜 기준)
H01_BASE_UNITS = {
    'r_voltage': 'V', 's_voltage': 'V', 't_voltage': 'V',
    'r_current': 'A', 's_current': 'A', 't_current': 'A',
    'ac_power': 'W', 'power_factor': '', 'frequency': 'Hz',
    'pv_voltage': 'V', 'pv_current': 'A', 'pv_power': 'W',
    'cumulative_energy': 'Wh', 'daily_energy': 'Wh',
}

# 단위 스케일 팩터 (k=1000, M=1000000 등)
_UNIT_SCALE_MAP = {
    ('kWh', 'Wh'): 1000, ('MWh', 'Wh'): 1000000, ('GWh', 'Wh'): 1000000000,
    ('kW', 'W'): 1000, ('MW', 'W'): 1000000,
    ('kVA', 'VA'): 1000, ('kVAr', 'VAr'): 1000,
    ('mA', 'A'): 0.001, ('mV', 'V'): 0.001,
}


def build_h01_match_table(categorized: dict, meta: dict) -> List[dict]:
    """V2: H01 Body 전체 필드 매칭 상태 + 단위/타입 검증"""
    rows = []
    max_mppt = meta.get('max_mppt', 0)
    max_string = meta.get('max_string', 0)

    # 1) DER 겹침 (9개) — 항상 O
    for h01_field, der_info in H01_DER_OVERLAP_FIELDS.items():
        expected_unit = H01_BASE_UNITS.get(h01_field, '')
        rows.append({
            'field': h01_field,
            'source': 'DER',
            'status': 'O',
            'type': 'S32', 'unit': expected_unit, 'scale': '0.1',
            'address': f'0x{der_info["addr_low"]:04X}~0x{der_info["addr_high"]:04X}',
            'definition': der_info['der_name'],
            'note': 'DER-AVM 고정 주소 사용',
        })

    # 2) Handler 계산 (pv_voltage, pv_current) — 항상 O
    for h01_field, rule in H01_HANDLER_COMPUTED_FIELDS.items():
        expected_unit = H01_BASE_UNITS.get(h01_field, '')
        rows.append({
            'field': h01_field,
            'source': 'HANDLER',
            'status': 'O',
            'address': '-',
            'definition': '-',
            'type': 'U16', 'unit': expected_unit, 'scale': '',
            'note': f'handler 계산: {rule}',
        })

    # 3) PDF 매핑 필요 (type/unit/unit_note 포함)
    pv_power_reg = _find_matched_reg(categorized, 'pv_power')
    rows.append(_make_pdf_match_row('pv_power', pv_power_reg, 'Total DC Power 미발견'))

    energy_reg = _find_matched_reg(categorized, 'cumulative_energy')
    if not energy_reg:
        energy_reg = _find_matched_reg(categorized, 'total_energy')
    rows.append(_make_pdf_match_row('cumulative_energy', energy_reg, 'Total Energy 미발견'))

    # cumulative_energy_low (소수부/Wh/Low Byte) — 있으면 매핑, 없으면 N/A
    energy_low_reg = _find_matched_reg(categorized, 'cumulative_energy_low')
    if energy_low_reg:
        rows.append(_make_pdf_match_row('cumulative_energy_low', energy_low_reg))
    else:
        rows.append({
            'field': 'cumulative_energy_low', 'source': 'PDF',
            'status': '-', 'address': '-', 'definition': '-',
            'type': '', 'unit': '', 'scale': '',
            'note': '소수부 레지스터 없음 (단일 레지스터)',
        })

    status_reg = _find_matched_reg(categorized, 'inverter_status', cat='STATUS')
    if not status_reg and categorized.get('STATUS'):
        # h01_field가 없는 STATUS 레지스터 중 Work Mode/Inverter Mode 키워드 검색
        for sr in categorized['STATUS']:
            dl = sr.definition.lower().replace('_', ' ')
            if any(k in dl for k in ['inverter mode', 'work mode', 'work state',
                                      'operating mode', 'running status',
                                      '시스템동작상태', '동작상태']):
                status_reg = sr
                break
    # 상태 정의(값 테이블) 검증 — 통상 Appendix에 별도 정의
    status_note = ''
    rows.append(_make_pdf_match_row('status', status_reg, 'Work State 미발견'))
    if status_note and rows[-1]['status'] == 'O':
        rows[-1]['note'] = status_note

    alarm_regs = categorized.get('ALARM', [])
    alarm_dist = distribute_alarms(alarm_regs)
    for slot in ['alarm1', 'alarm2', 'alarm3']:
        regs = alarm_dist.get(slot, [])
        if regs:
            rows.append(_make_pdf_match_row(slot, regs[0]))
        else:
            rows.append({
                'field': slot, 'source': 'PDF',
                'status': '-' if slot != 'alarm1' else 'X',
                'address': '-', 'definition': '-', 'type': '', 'unit': '', 'scale': '',
                'note': '보조 알람 없음' if slot != 'alarm1' else '알람 미발견',
            })

    for n in range(1, max_mppt + 1):
        for mtype in ['voltage', 'current']:
            field = f'mppt{n}_{mtype}'
            reg = _find_matched_reg(categorized, field)
            rows.append(_make_pdf_match_row(field, reg))

    for n in range(1, max_string + 1):
        field = f'string{n}_current'
        reg = _find_matched_reg(categorized, field)
        rows.append(_make_pdf_match_row(field, reg, 'String current 미지원 시 생략'))

    return rows


def _find_matched_reg(categorized: dict, h01_field: str, cat: str = None) -> Optional[RegisterRow]:
    search_cats = [cat] if cat else ['MONITORING', 'INFO', 'STATUS', 'ALARM']
    for c in search_cats:
        for reg in categorized.get(c, []):
            if reg.h01_field == h01_field:
                return reg
    return None


def _get_unit_scale(reg_unit: str, h01_field: str) -> tuple:
    """레지스터 단위 → (H01 기본단위 변환 스케일, 변환 설명)"""
    base = H01_BASE_UNITS.get(h01_field, '')
    ru = (reg_unit or '').strip()
    if not base or not ru or ru == base:
        return (1, '')
    # k/M/G 접두사 변환
    for (src, dst), factor in _UNIT_SCALE_MAP.items():
        if ru.upper() == src.upper() and dst.upper() == base.upper():
            return (factor, f'{ru}→{base} (*{factor})')
    # 스케일 포함된 단위 (예: 0.1V → V)
    return (1, '')


def _make_pdf_match_row(h01_field: str, reg, miss_note: str = '') -> dict:
    """PDF 매칭 행 생성 — scale은 PDF 원문 그대로, 정의 테이블 포함"""
    if reg:
        _, unit_desc = _get_unit_scale(reg.unit, h01_field)
        # 정의 요약 (value_definitions가 있으면)
        val_defs = getattr(reg, 'value_definitions', None)
        val_defs_str = ''
        if val_defs and isinstance(val_defs, dict):
            items = sorted(val_defs.items())[:6]
            val_defs_str = ', '.join(f'{k}={v[:15]}' for k, v in items)
            if len(val_defs) > 6:
                val_defs_str += f'... (+{len(val_defs)-6})'
        note = unit_desc
        if val_defs_str:
            note = f'{val_defs_str}' + (f' | {unit_desc}' if unit_desc else '')
        return {
            'field': h01_field, 'source': 'PDF', 'status': 'O',
            'address': reg.address_hex, 'definition': reg.definition,
            'type': reg.data_type, 'unit': reg.unit or '',
            'scale': reg.scale or '',
            'note': note,
            'value_definitions': val_defs,
        }
    else:
        return {
            'field': h01_field, 'source': 'PDF', 'status': 'X',
            'address': '-', 'definition': '-',
            'type': '', 'unit': '', 'scale': '',
            'note': miss_note,
        }


# ─── Stage 1 메인 ───────────────────────────────────────────────────────────

def run_stage1(
    input_path: str,
    output_dir: str,
    device_type: str = 'inverter',
    progress: ProgressCallback = None,
) -> dict:
    """Stage 1: PDF/Excel → Stage 1 Excel (MAPPING_RULES_V2)"""
    def log(msg, level='info'):
        if progress:
            progress(msg, level)

    openpyxl = get_openpyxl()
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    log('레퍼런스 로딩 중...')
    synonym_db = load_synonym_db()
    review_history = load_review_history()
    ref_patterns = load_reference_patterns()
    log(f'  synonym_db: {len(synonym_db.get("fields", {}))}개 필드')
    log(f'  review_history: {len(review_history.get("approved", []))}개 이력')
    log(f'  레퍼런스: {len(ref_patterns)}개 프로토콜')

    ext = os.path.splitext(input_path)[1].lower()
    basename = os.path.splitext(os.path.basename(input_path))[0]

    log(f'입력 파일 읽기: {os.path.basename(input_path)}')
    all_tables = []
    pages = None  # PDF만 사용, Excel은 None

    if ext == '.pdf':
        pages = extract_pdf_text_and_tables(input_path)
        log(f'  PDF {len(pages)}페이지 추출')

        # 섹션 감지: 2단계 — 구체적 테이블 제목 우선, 일반 키워드 fallback
        import re as _re
        # 1순위: 데이터 테이블 제목 (줄 시작, "table N" 또는 섹션 번호 포함)
        _INPUT_TABLE_RE = _re.compile(
            r'(?:^|\n)\s*(?:table\s+\d+\s+)?input\s+register\s*(?:mapping|table|list)?',
            _re.I | _re.MULTILINE)
        _HOLDING_TABLE_RE = _re.compile(
            r'(?:^|\n)\s*(?:table\s+\d+\s+)?hold(?:ing)?\s+register\s*(?:mapping|table|list)?',
            _re.I | _re.MULTILINE)
        # 2순위: 일반 섹션 키워드 (기존)
        _INPUT_KEYWORDS = ['input register mapping', 'input (read only)',
                           'read only register', 'running information variable address',
                           'running information']
        _HOLDING_KEYWORDS = ['hold register mapping', 'holding register mapping',
                             'parameter setting address definition', 'parameter setting']

        section_input_start = None
        section_holding_start = None

        # 1순위: 테이블 제목 패턴 (가장 신뢰)
        for p in pages:
            pnum = p['page']
            if section_input_start is None and _INPUT_TABLE_RE.search(p['text']):
                # 해당 페이지에 실제 데이터 테이블이 있어야 함
                has_data = any(len(tab) >= 3 for tab in p.get('tables', []))
                if has_data:
                    section_input_start = pnum
            if section_holding_start is None and _HOLDING_TABLE_RE.search(p['text']):
                has_data = any(len(tab) >= 3 for tab in p.get('tables', []))
                if has_data and pnum != section_input_start:
                    section_holding_start = pnum

        # 2순위: 일반 키워드 (1순위에서 못 찾은 경우만)
        if section_input_start is None:
            for p in pages:
                if any(m in p['text'].lower() for m in _INPUT_KEYWORDS):
                    has_data = any(len(tab) >= 3 for tab in p.get('tables', []))
                    if has_data:
                        section_input_start = p['page']
                        break
        if section_holding_start is None:
            for p in pages:
                if any(m in p['text'].lower() for m in _HOLDING_KEYWORDS):
                    has_data = any(len(tab) >= 3 for tab in p.get('tables', []))
                    if has_data and p['page'] != section_input_start:
                        section_holding_start = p['page']
                        break

        # 섹션 순서 결정 (어느 것이 먼저 오는지)
        tables_input, tables_holding, tables_other = [], [], []
        for p in pages:
            pnum = p['page']
            if section_input_start and section_holding_start:
                # 둘 다 감지된 경우 — 페이지 순서로 구분
                if section_input_start < section_holding_start:
                    in_input = (pnum >= section_input_start and pnum < section_holding_start)
                    in_holding = (pnum >= section_holding_start)
                else:
                    in_holding = (pnum >= section_holding_start and pnum < section_input_start)
                    in_input = (pnum >= section_input_start)
            elif section_input_start:
                in_input = (pnum >= section_input_start)
                in_holding = False
            elif section_holding_start:
                in_holding = (pnum >= section_holding_start)
                in_input = False
            else:
                in_input = in_holding = False

            for tab in p['tables']:
                if in_holding:
                    tables_holding.append(tab)
                elif in_input:
                    tables_input.append(tab)
                else:
                    tables_other.append(tab)

        # FC 태깅된 테이블 리스트 생성
        all_tables = []
        fc_list = []
        for tab in tables_input:
            all_tables.append(tab); fc_list.append('04')
        for tab in tables_other:
            all_tables.append(tab); fc_list.append('')
        for tab in tables_holding:
            all_tables.append(tab); fc_list.append('03')

        has_fc = bool(section_input_start and section_holding_start)
        log(f'  Input(FC04): {len(tables_input)}개 (page {section_input_start}~), '
            f'Holding(FC03): {len(tables_holding)}개 (page {section_holding_start}~), '
            f'기타: {len(tables_other)}개'
            f'{" — FC 구분 활성" if has_fc else ""}')

    elif ext in ('.xlsx', '.xls'):
        fc_list = []
        sheets = extract_excel_sheets(input_path)
        log(f'  Excel {len(sheets)}시트 추출')
        for sname, rows in sheets.items():
            if rows:
                all_tables.append(rows)
                fc_list.append('')  # Excel은 FC 구분 없음
    else:
        raise ValueError(f'지원하지 않는 파일 형식: {ext}')

    log('레지스터 테이블 파싱...')
    registers = extract_registers_from_tables(all_tables, fc_list=fc_list if fc_list else None)
    log(f'  {len(registers)}개 레지스터 추출 (원본)')

    if not registers:
        raise ValueError('레지스터를 찾지 못했습니다. PDF/Excel 형식을 확인해주세요.')

    manufacturer = basename.split('_')[0].split(' ')[0]
    log(f'  제조사 (파일명 기반): {manufacturer}')

    DER_FIXED_ADDRS = set()
    for dr in DER_CONTROL_REGS:
        DER_FIXED_ADDRS.add(dr['addr'])
    for dr in DER_MONITOR_REGS:
        DER_FIXED_ADDRS.add(dr['addr'])

    before = len(registers)
    registers = [r for r in registers
                 if (r.address if isinstance(r.address, int) else parse_address(r.address))
                 not in DER_FIXED_ADDRS]
    if before - len(registers):
        log(f'  DER 고정 주소 제외: {before - len(registers)}개')

    # ── INFO 블록 사전 감지 (enrichment 전 — PDF 원본 이름 기준) ──
    info_block = detect_info_block(registers, pages=pages)
    info_addrs = info_block['info_addrs']
    log(f'  INFO 블록: {len(info_addrs)}개 레지스터, '
        f'Model={"✓" if info_block["model_found"] else "✗ MISSING"}, '
        f'SN={"✓" if info_block["sn_found"] else "✗ MISSING"}')

    if ref_patterns:
        mfr_lower = manufacturer.lower()
        matched_ref = {k: v for k, v in ref_patterns.items() if mfr_lower in k.lower()}
        if matched_ref:
            enriched = 0
            for reg in registers:
                addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)
                if addr is None:
                    continue
                ref_name = None
                for proto, addr_map in matched_ref.items():
                    if addr in addr_map:
                        ref_name = addr_map[addr]
                        break
                if ref_name:
                    original = reg.definition
                    reg.definition = ref_name
                    if reg.comment and original != ref_name:
                        reg.comment = f'{original} | {reg.comment}'
                    elif original != ref_name:
                        reg.comment = original
                    enriched += 1
            if enriched:
                log(f'  레퍼런스 enrichment: {enriched}/{len(registers)}개 ({list(matched_ref.keys())})')
        else:
            log(f'  레퍼런스 enrichment: 해당 제조사({manufacturer}) 없음')

    normalized = 0
    for reg in registers:
        if detect_channel_number(reg.definition):
            continue
        syn = match_synonym(reg.definition, synonym_db)
        if syn and syn['field'] != to_upper_snake(reg.definition):
            original = reg.definition
            reg.definition = syn['field']
            if reg.comment and original != syn['field']:
                reg.comment = f'{original} | {reg.comment}'
            elif original != syn['field']:
                reg.comment = original
            normalized += 1
    if normalized:
        log(f'  synonym_db 정규화: {normalized}개')

    seen_names = {}
    for i, reg in enumerate(registers):
        name = to_upper_snake(reg.definition)
        # 이름+단위 조합으로 중복 체크 (같은 이름이라도 단위 다르면 유지)
        name_key = f'{name}_{reg.unit}' if reg.unit else name
        if name_key in seen_names:
            registers[i] = None
        else:
            seen_names[name_key] = i
    registers = [r for r in registers if r is not None]
    log(f'  중복 제거 후: {len(registers)}개')

    protocol_version = ''
    if ext == '.pdf':
        full_text = ' '.join(p['text'] for p in pages[:3])
        m = re.search(r'[Vv](\d+\.\d+(?:\.\d+)?)', full_text)
        if m:
            protocol_version = f'V{m.group(1)}'

    max_mppt = 0
    max_string = 0
    for reg in registers:
        ch = detect_channel_number(reg.definition)
        if not ch:
            addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)
            if addr is not None:
                ch = detect_channel_from_ref(addr, ref_patterns)
        if ch:
            prefix, n = ch
            if prefix == 'MPPT':
                max_mppt = max(max_mppt, n)
            elif prefix == 'STRING':
                max_string = max(max_string, n)

    # V2: MPPT수 > String수이면 PDF 버그 — MPPT수를 String수로 캡
    if max_string > 0 and max_mppt > max_string:
        max_mppt = max_string

    # V2: IV Scan 지원 = IV 데이터 레지스터(Tracker/PV point)가 있어야 함
    # IV command(0x600D)만 있고 데이터 레지스터 없으면 미지원 (예: Huawei)
    iv_info = detect_iv_from_pdf(registers)
    iv_scan_supported = iv_info['supported'] and len(iv_info.get('trackers', [])) > 0

    # 확정 제조사 + 데이터 레지스터 없으면 keyword fallback
    if not iv_scan_supported and manufacturer.lower() in ('solarize', 'kstar', 'senergy'):
        iv_scan_supported = True
        iv_info['supported'] = True

    # IV command는 DER 고정 주소(0x600D)에서 제거되므로, 지원 시 고정값 삽입
    if iv_scan_supported and not iv_info.get('iv_command_addr'):
        iv_info['iv_command_addr'] = 0x600D
        iv_info['supported'] = True

    meta = {
        'manufacturer': manufacturer,
        'protocol_version': protocol_version,
        'device_type': device_type,
        'max_mppt': max_mppt,
        'max_string': max_string,
        'iv_scan': iv_scan_supported and device_type == 'inverter',
        'iv_data_points': iv_info.get('data_points', 0),
        'iv_trackers': len(iv_info.get('trackers', [])),
        'iv_command_addr': f'0x{iv_info["iv_command_addr"]:04X}' if iv_info.get('iv_command_addr') else '-',
        'source_file': os.path.basename(input_path),
        'extracted_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'total_extracted': len(registers),
        'mapping_rules': 'V2',
    }

    log(f'  제조사: {manufacturer}, MPPT: {max_mppt}, String: {max_string}')
    if meta['iv_scan']:
        log(f'  IV Scan: Yes (command={meta["iv_command_addr"]}, '
            f'trackers={meta["iv_trackers"]}, data_points={meta["iv_data_points"]})')
    else:
        log(f'  IV Scan: No')

    # INFO 블록은 enrichment 전에 감지됨 (info_addrs 이미 설정)

    log('카테고리 분류 중 (MAPPING_RULES_V2)...')
    categorized = {cat: [] for cat in
                   ['INFO', 'MONITORING', 'STATUS', 'ALARM',
                    'DER_CONTROL', 'DER_MONITOR', 'IV_SCAN', 'REVIEW']}
    excluded = []

    for reg in registers:
        cat, reason = classify_register_with_rules(
            reg, synonym_db, review_history, ref_patterns,
            device_type, all_regs=registers, info_addrs=info_addrs)
        if cat == 'EXCLUDE':
            excluded.append(reg)
            continue
        reg.category = cat
        reg.h01_field = assign_h01_field(reg, synonym_db, ref_patterns)

        if cat == 'REVIEW':
            reg.review_reason = reason or '자동 분류 불가'
            fuzzy = match_synonym_fuzzy(reg.definition, synonym_db, threshold=0.4)
            if fuzzy:
                reg.review_suggestion = f'{fuzzy["category"]}/{fuzzy["field"]} (유사도 {fuzzy["score"]:.0%})'
            else:
                reg.review_suggestion = '분류 불가'

        if cat in categorized:
            categorized[cat].append(reg)

    if device_type == 'inverter':
        categorized['DER_CONTROL'] = []
        categorized['DER_MONITOR'] = []
        for dr in DER_CONTROL_REGS:
            categorized['DER_CONTROL'].append(RegisterRow(
                definition=dr['name'], address=dr['addr'],
                data_type=dr['type'], scale=dr.get('scale', ''),
                rw=dr['rw'], comment=dr['desc'], category='DER_CONTROL'))
        for dr in DER_MONITOR_REGS:
            categorized['DER_MONITOR'].append(RegisterRow(
                definition=dr['name'], address=dr['addr'],
                data_type=dr['type'], scale=dr.get('scale', ''),
                unit=dr.get('unit', ''), rw='RO',
                comment=dr.get('desc', ''), category='DER_MONITOR'))

    if device_type != 'inverter':
        categorized['DER_CONTROL'] = []
        categorized['DER_MONITOR'] = []
        categorized['IV_SCAN'] = []

    counts = {cat: len(regs) for cat, regs in categorized.items()}
    log('분류 결과:')
    for cat, cnt in counts.items():
        if cnt > 0:
            log(f'  {cat:15s}: {cnt}개')
    log(f'  제외: {len(excluded)}개')

    # ── 정의 테이블 탐색 (참고용 — 레지스터 연결은 추후 정밀도 개선 후) ──
    # def_tables = scan_definition_tables(pages, ...) — 정밀도 개선 시 활성화

    # ── INFO Appendix 테이블 추출 ──
    info_appendix = {}
    if pages:
        info_appendix = scan_info_appendix_tables(
            categorized.get('INFO', []), pages)
        for app_id, app in info_appendix.items():
            log(f'  Appendix {app_id}: {len(app["rows"])}개 모델 (page {app["page"]})')

    h01_match_table = build_h01_match_table(categorized, meta)
    h01_matched = sum(1 for r in h01_match_table if r['status'] == 'O')
    h01_total = len(h01_match_table)
    log(f'H01 매칭: {h01_matched}/{h01_total}')

    # ── DER 매칭 테이블 생성 ──
    der_match_table = _build_der_match_table(categorized)
    der_matched = sum(1 for r in der_match_table if r['status'] == 'O')
    der_total = len(der_match_table)
    log(f'DER 매칭: {der_matched}/{der_total}')

    output_name = f'test_{basename}_stage1.xlsx'
    output_path = os.path.join(output_dir, output_name)
    log(f'Excel 생성: {output_name}')

    wb = openpyxl.Workbook()
    thin_border = Border(
        left=Side(style='thin'), right=Side(style='thin'),
        top=Side(style='thin'), bottom=Side(style='thin'))
    hdr_font = Font(bold=True, color='FFFFFF')
    hdr_fill = PatternFill('solid', fgColor='333333')
    title_font = Font(bold=True, size=14)
    section_font = Font(bold=True, size=12, color='1F4E79')

    def _write_header(ws, columns, row=1):
        for j, col_name in enumerate(columns, start=1):
            cell = ws.cell(row=row, column=j, value=col_name)
            cell.font = hdr_font
            cell.fill = hdr_fill
            cell.border = thin_border

    # ═══════════════════════════════════════════════════════════════════
    # Sheet 1: INFO — 인포 매칭 & 메타데이터
    # ═══════════════════════════════════════════════════════════════════
    ws = wb.active
    ws.title = '1_INFO'

    # 메타데이터 섹션
    ws['A1'] = 'Stage 1 — 레지스터맵 추출 (MAPPING_RULES_V2)'
    ws['A1'].font = title_font
    meta_items = [
        ('제조사', manufacturer), ('프로토콜 버전', protocol_version),
        ('설비 타입', device_type), ('MPPT', max_mppt),
        ('String', max_string),
        ('IV Scan', 'Yes' if meta['iv_scan'] else 'No'),
        ('IV Data Points', meta.get('iv_data_points', 0) if meta['iv_scan'] else '-'),
        ('IV Trackers', meta.get('iv_trackers', 0) if meta['iv_scan'] else '-'),
        ('원본 파일', os.path.basename(input_path)), ('추출 일시', meta['extracted_date']),
        ('H01 매칭', f'{h01_matched}/{h01_total}'),
        ('DER 매칭', f'{der_matched}/{der_total}'),
        ('추출 레지스터', meta['total_extracted']),
        ('REVIEW', counts.get('REVIEW', 0)),
    ]
    for i, (k, v) in enumerate(meta_items, start=3):
        ws[f'A{i}'] = k
        ws[f'A{i}'].font = Font(bold=True)
        ws[f'B{i}'] = str(v)

    # INFO 레지스터 섹션
    info_start = len(meta_items) + 5
    ws.cell(row=info_start, column=1, value='INFO 레지스터').font = section_font
    info_cols = ['No', 'Definition', 'Address', 'FC', 'Type', 'Unit/Scale', 'R/W', 'H01 Field', 'Comment']
    _write_header(ws, info_cols, info_start + 1)

    info_regs = sorted(categorized.get('INFO', []),
                       key=lambda r: (r.address if isinstance(r.address, int) else 0))
    for i, reg in enumerate(info_regs, start=1):
        su = f'{reg.unit} {reg.scale}'.strip() if reg.unit or reg.scale else ''
        fc_str = f'0x{reg.fc}' if reg.fc else ''
        vals = [i, reg.definition, reg.address_hex, fc_str, reg.data_type, su,
                reg.rw, reg.h01_field or '', reg.comment]
        for j, val in enumerate(vals, start=1):
            cell = ws.cell(row=info_start + 1 + i, column=j, value=val)
            cell.border = thin_border
            cell.fill = PatternFill('solid', fgColor=CATEGORY_COLORS['INFO'])

    # STATUS 섹션
    status_start = info_start + len(info_regs) + 4
    ws.cell(row=status_start, column=1, value='STATUS 레지스터').font = section_font
    _write_header(ws, info_cols, status_start + 1)

    status_regs = sorted(categorized.get('STATUS', []),
                         key=lambda r: (r.address if isinstance(r.address, int) else 0))
    for i, reg in enumerate(status_regs, start=1):
        su = f'{reg.unit} {reg.scale}'.strip() if reg.unit or reg.scale else ''
        fc_str = f'0x{reg.fc}' if reg.fc else ''
        vals = [i, reg.definition, reg.address_hex, fc_str, reg.data_type, su,
                reg.rw, reg.h01_field or '', reg.comment]
        for j, val in enumerate(vals, start=1):
            cell = ws.cell(row=status_start + 1 + i, column=j, value=val)
            cell.border = thin_border
            cell.fill = PatternFill('solid', fgColor=CATEGORY_COLORS['STATUS'])

    # ALARM 섹션
    alarm_start = status_start + len(status_regs) + 4
    ws.cell(row=alarm_start, column=1, value='ALARM 레지스터').font = section_font
    _write_header(ws, info_cols, alarm_start + 1)

    alarm_regs_sorted = sorted(categorized.get('ALARM', []),
                               key=lambda r: (r.address if isinstance(r.address, int) else 0))
    for i, reg in enumerate(alarm_regs_sorted, start=1):
        su = f'{reg.unit} {reg.scale}'.strip() if reg.unit or reg.scale else ''
        fc_str = f'0x{reg.fc}' if reg.fc else ''
        vals = [i, reg.definition, reg.address_hex, fc_str, reg.data_type, su,
                reg.rw, reg.h01_field or '', reg.comment]
        for j, val in enumerate(vals, start=1):
            cell = ws.cell(row=alarm_start + 1 + i, column=j, value=val)
            cell.border = thin_border
            cell.fill = PatternFill('solid', fgColor=CATEGORY_COLORS['ALARM'])

    # REVIEW 섹션 (있으면)
    review_regs = categorized.get('REVIEW', [])
    if review_regs:
        review_start = alarm_start + len(alarm_regs_sorted) + 4
        ws.cell(row=review_start, column=1, value=f'REVIEW ({len(review_regs)}개)').font = section_font
        review_cols = ['No', 'Definition', 'Address', 'Type', 'Unit/Scale', 'R/W', '사유', '제안']
        _write_header(ws, review_cols, review_start + 1)
        for i, reg in enumerate(sorted(review_regs, key=lambda r: (r.address if isinstance(r.address, int) else 0)),
                                start=1):
            su = f'{reg.unit} {reg.scale}'.strip() if reg.unit or reg.scale else ''
            vals = [i, reg.definition, reg.address_hex, reg.data_type, su,
                    reg.rw, reg.review_reason, reg.review_suggestion]
            for j, val in enumerate(vals, start=1):
                cell = ws.cell(row=review_start + 1 + i, column=j, value=val)
                cell.border = thin_border
                cell.fill = PatternFill('solid', fgColor=CATEGORY_COLORS['REVIEW'])

    # ── INFO Appendix 테이블 출력 ──
    if info_appendix:
        # 마지막 섹션 이후 위치 계산
        last_row = ws.max_row + 3
        appendix_fill = PatternFill('solid', fgColor='D6E4F0')  # 연한 파란색
        for app_id, app in info_appendix.items():
            ws.cell(row=last_row, column=1,
                    value=f'{app["title"]} — {app["register"]}').font = section_font
            last_row += 1
            # 헤더
            for j, col_name in enumerate(app['header'], start=1):
                cell = ws.cell(row=last_row, column=j, value=col_name)
                cell.font = hdr_font
                cell.fill = hdr_fill
                cell.border = thin_border
            last_row += 1
            # 데이터 행
            for row_data in app['rows']:
                for j, val in enumerate(row_data, start=1):
                    cell = ws.cell(row=last_row, column=j, value=val)
                    cell.border = thin_border
                    cell.fill = appendix_fill
                last_row += 1
            last_row += 2  # 간격

    ws.column_dimensions['A'].width = 18
    ws.column_dimensions['B'].width = 40
    ws.column_dimensions['C'].width = 12
    ws.column_dimensions['G'].width = 18
    ws.column_dimensions['H'].width = 35

    # ═══════════════════════════════════════════════════════════════════
    # Sheet 2: H01_MATCH — H01 Body 필드 매칭
    # ═══════════════════════════════════════════════════════════════════
    ws_h01 = wb.create_sheet('2_H01')
    ws_h01['A1'] = f'H01 모니터링 매칭 — {h01_matched}/{h01_total}'
    ws_h01['A1'].font = title_font

    h01_cols = ['No', 'H01 Field', 'Source', 'Status', 'Address', 'Definition', 'Type', 'Unit', 'Scale', 'Note']
    _write_header(ws_h01, h01_cols, 3)

    for i, rd in enumerate(h01_match_table, start=1):
        vals = [i, rd['field'], rd['source'], rd['status'],
                rd['address'], rd['definition'],
                rd.get('type', ''), rd.get('unit', ''), rd.get('scale', ''), rd.get('note', '')]
        sc = MATCH_COLORS.get(rd['status'], 'FFFFFF')
        for j, val in enumerate(vals, start=1):
            cell = ws_h01.cell(row=3 + i, column=j, value=val)
            cell.border = thin_border
            if j == 4:
                cell.fill = PatternFill('solid', fgColor=sc)
            elif rd['source'] == 'DER':
                cell.fill = PatternFill('solid', fgColor='D9D2E9')
            elif rd['source'] == 'HANDLER':
                cell.fill = PatternFill('solid', fgColor='FCE5CD')

    # MONITORING 전체 목록
    mon_start = 3 + len(h01_match_table) + 3
    ws_h01.cell(row=mon_start, column=1,
                value=f'MONITORING 전체 ({len(categorized.get("MONITORING", []))}개)').font = section_font
    mon_cols = ['No', 'Definition', 'Address', 'Type', 'Unit/Scale', 'R/W', 'H01 Field']
    _write_header(ws_h01, mon_cols, mon_start + 1)

    mon_regs = sorted(categorized.get('MONITORING', []),
                      key=lambda r: (r.address if isinstance(r.address, int) else 0))
    for i, reg in enumerate(mon_regs, start=1):
        su = f'{reg.unit} {reg.scale}'.strip() if reg.unit or reg.scale else ''
        vals = [i, reg.definition, reg.address_hex, reg.data_type, su,
                reg.rw, reg.h01_field or '']
        for j, val in enumerate(vals, start=1):
            cell = ws_h01.cell(row=mon_start + 1 + i, column=j, value=val)
            cell.border = thin_border
            cell.fill = PatternFill('solid', fgColor=CATEGORY_COLORS['MONITORING'])

    ws_h01.column_dimensions['B'].width = 25
    ws_h01.column_dimensions['E'].width = 20
    ws_h01.column_dimensions['F'].width = 40
    ws_h01.column_dimensions['G'].width = 40

    # ═══════════════════════════════════════════════════════════════════
    # Sheet 3: DER — DER-AVM 매칭
    # ═══════════════════════════════════════════════════════════════════
    if device_type == 'inverter':
        ws_der = wb.create_sheet('3_DER')
        ws_der['A1'] = f'DER-AVM 매칭 — {der_matched}/{der_total}'
        ws_der['A1'].font = title_font

        der_cols = ['No', 'Field', 'Type', 'Status', 'Address', 'Scale', 'R/W', 'Description']
        _write_header(ws_der, der_cols, 3)

        for i, rd in enumerate(der_match_table, start=1):
            vals = [i, rd['name'], rd['type'], rd['status'],
                    rd['address'], rd['scale'], rd['rw'], rd['desc']]
            sc = MATCH_COLORS.get(rd['status'], 'FFFFFF')
            cat_color = CATEGORY_COLORS['DER_CONTROL'] if rd['group'] == 'CONTROL' else CATEGORY_COLORS['DER_MONITOR']
            for j, val in enumerate(vals, start=1):
                cell = ws_der.cell(row=3 + i, column=j, value=val)
                cell.border = thin_border
                if j == 4:
                    cell.fill = PatternFill('solid', fgColor=sc)
                else:
                    cell.fill = PatternFill('solid', fgColor=cat_color)

        ws_der.column_dimensions['B'].width = 35
        ws_der.column_dimensions['E'].width = 15
        ws_der.column_dimensions['H'].width = 45

    # ═══════════════════════════════════════════════════════════════════
    # Sheet 4: IV — IV 스캔 매핑 (지원 시)
    # ═══════════════════════════════════════════════════════════════════
    if meta['iv_scan'] and iv_info.get('supported'):
        ws_iv = wb.create_sheet('4_IV')
        ws_iv['A1'] = f'IV Scan 매핑 — data_points={iv_info["data_points"]}'
        ws_iv['A1'].font = title_font

        iv_fill = PatternFill('solid', fgColor=CATEGORY_COLORS['IV_SCAN'])
        ctrl_fill = PatternFill('solid', fgColor='D9D2E9')

        # IV 메타 정보
        iv_meta = [
            ('IV Scan Command', meta.get('iv_command_addr', '-')),
            ('Data Points', iv_info['data_points']),
            ('Trackers', len(iv_info['trackers'])),
            ('Strings/Tracker', max(len(t['strings']) for t in iv_info['trackers']) if iv_info['trackers'] else 0),
        ]
        for i, (k, v) in enumerate(iv_meta, start=3):
            ws_iv[f'A{i}'] = k
            ws_iv[f'A{i}'].font = Font(bold=True)
            ws_iv[f'B{i}'] = str(v)

        # IV Command 레지스터
        cmd_start = len(iv_meta) + 5
        ws_iv.cell(row=cmd_start, column=1, value='IV Scan Command').font = section_font
        cmd_cols = ['Name', 'Address', 'Type', 'R/W', 'Description']
        _write_header(ws_iv, cmd_cols, cmd_start + 1)
        cmd_addr = iv_info.get('iv_command_addr')
        cmd_vals = ['IV_CURVE_SCAN', f'0x{cmd_addr:04X}' if cmd_addr else '-', 'U16', 'RW',
                    'W: 0=Stop, 1=Start / R: 0=Idle, 1=Running, 2=Finished']
        for j, val in enumerate(cmd_vals, start=1):
            cell = ws_iv.cell(row=cmd_start + 2, column=j, value=val)
            cell.border = thin_border
            cell.fill = ctrl_fill

        # Tracker/String 매핑 테이블
        map_start = cmd_start + 5
        ws_iv.cell(row=map_start, column=1, value='IV Data 레지스터 매핑').font = section_font
        map_cols = ['No', 'Type', 'Name', 'Address', 'Regs', 'Data Type', 'Scale']
        _write_header(ws_iv, map_cols, map_start + 1)

        row_idx = map_start + 2
        reg_no = 1
        for tracker in iv_info['trackers']:
            tn = tracker['tracker_num']
            va = tracker['voltage_addr']
            # Tracker voltage
            vals = [reg_no, 'Tracker Voltage', f'Tracker {tn} voltage',
                    f'0x{va:04X}' if va else '-', iv_info['data_points'], 'U16', '0.1V']
            for j, val in enumerate(vals, start=1):
                cell = ws_iv.cell(row=row_idx, column=j, value=val)
                cell.border = thin_border
                cell.fill = PatternFill('solid', fgColor='D5F5E3')  # 연한 초록
            row_idx += 1
            reg_no += 1

            # String currents
            for sc in tracker['strings']:
                sn = sc['string_num']
                sa = sc['current_addr']
                vals = [reg_no, 'String Current', f'String {tn}-{sn} current',
                        f'0x{sa:04X}', iv_info['data_points'], 'S16', '0.01A']
                for j, val in enumerate(vals, start=1):
                    cell = ws_iv.cell(row=row_idx, column=j, value=val)
                    cell.border = thin_border
                    cell.fill = iv_fill
                row_idx += 1
                reg_no += 1

        ws_iv.column_dimensions['A'].width = 6
        ws_iv.column_dimensions['B'].width = 18
        ws_iv.column_dimensions['C'].width = 25
        ws_iv.column_dimensions['D'].width = 12
        ws_iv.column_dimensions['E'].width = 10

    wb.save(output_path)
    wb.close()

    sheet_count = 3 + (1 if meta['iv_scan'] and iv_info.get('supported') else 0)
    log(f'Stage 1 완료: {output_name} ({sheet_count}시트)', 'ok')

    return {
        'output_path': output_path,
        'output_name': output_name,
        'counts': counts,
        'meta': meta,
        'review_count': counts.get('REVIEW', 0),
        'h01_match': {'matched': h01_matched, 'total': h01_total, 'table': h01_match_table},
        'der_match': {'matched': der_matched, 'total': der_total},
        'iv_info': iv_info,
        'info_appendix': info_appendix,
    }


def _link_definitions_to_registers(categorized: dict, def_tables: dict):
    """정의 테이블을 STATUS/ALARM 레지스터에 연결"""
    # STATUS 레지스터에 정의 연결
    for reg in categorized.get('STATUS', []):
        addr = reg.address if isinstance(reg.address, int) else 0
        defn_lower = reg.definition.lower().replace('_', ' ')
        comment_lower = (reg.comment or '').lower()

        for d in def_tables['status_defs']:
            # 1) 주소 매칭
            if d.get('address') and d['address'] == addr:
                reg.value_definitions = d['values']
                break
            # 2) 이름 매칭: comment에 인라인 정의가 있는 경우
            if d.get('address') == addr and addr > 0:
                reg.value_definitions = d['values']
                break
            # 3) 키워드 매칭: mode/status 레지스터 + 정의 타입 일치
            if d['type'] == 'mode_table' and not d.get('address'):
                if any(k in defn_lower for k in ['inverter mode', 'work mode', 'work state',
                                                   'operating mode', 'running status',
                                                   '시스템동작상태', 'device status']):
                    if not getattr(reg, 'value_definitions', None):
                        reg.value_definitions = d['values']

    # ALARM 레지스터에 정의 연결
    for reg in categorized.get('ALARM', []):
        addr = reg.address if isinstance(reg.address, int) else 0
        defn_lower = reg.definition.lower().replace('_', ' ')
        comment_lower = (reg.comment or '').lower()

        for d in def_tables['alarm_defs']:
            # 1) 주소 매칭
            if d.get('address') and d['address'] == addr:
                reg.value_definitions = d['values']
                break
            # 2) 이름 키워드 매칭 (Error Code 1 → Error Code Table1)
            if not d.get('address'):
                d_name_lower = d['name'].lower()
                # "Error Code Table1" ↔ "Error Code1"
                if ('error code' in defn_lower and 'error code' in d_name_lower) or \
                   ('alarm code' in defn_lower and 'alarm' in d_name_lower) or \
                   ('dsp alarm' in defn_lower and 'dsp alarm' in d_name_lower) or \
                   ('dsp error' in defn_lower and 'dsp error' in d_name_lower) or \
                   ('arm alarm' in defn_lower and 'arm alarm' in d_name_lower) or \
                   ('fault status' in defn_lower and 'fault' in d_name_lower) or \
                   ('error message' in defn_lower and 'error' in d_name_lower):
                    if not getattr(reg, 'value_definitions', None):
                        reg.value_definitions = d['values']


def _build_der_match_table(categorized: dict) -> List[dict]:
    """DER-AVM 매칭 테이블 — 고정 주소맵 기반"""
    rows = []
    for dr in DER_CONTROL_REGS:
        rows.append({
            'name': dr['name'], 'type': dr['type'],
            'address': f'0x{dr["addr"]:04X}', 'scale': dr.get('scale', ''),
            'rw': dr['rw'], 'desc': dr['desc'],
            'status': 'O', 'group': 'CONTROL',
        })
    for dr in DER_MONITOR_REGS:
        rows.append({
            'name': dr['name'], 'type': dr['type'],
            'address': f'0x{dr["addr"]:04X}', 'scale': dr.get('scale', ''),
            'rw': 'RO', 'desc': dr.get('desc', ''),
            'status': 'O', 'group': 'MONITOR',
        })
    return rows
