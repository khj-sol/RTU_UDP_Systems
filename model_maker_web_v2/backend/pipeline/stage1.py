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
    classify_register_with_rules, detect_iv_scan_support,
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
_TYPE_RE = re.compile(r'\b(U16|S16|U32|S32|I16|I32|INT16|UINT16|INT32|UINT32|FLOAT32|ASCII|STRING|STR|Bitfield16|Bitfield32)\b', re.I)
_RW_RE   = re.compile(r'\b(R/?W|RO|WO|Read|Write|R/W)\b', re.I)
_SCALE_RE = re.compile(r'(?:scale|factor|×|x)\s*[=:]?\s*([\d.]+)', re.I)
_UNIT_RE = re.compile(r'\b(V|A|W|kW|KW|VA|kVA|KVA|VAr|kVAr|KVar|Hz|°C|℃|Wh|kWh|KWh|Kwh|KWH|MWh|MWH|%)\b')


def _detect_table_columns(header_row: list, data_rows: list = None) -> dict:
    """테이블 헤더에서 컬럼 인덱스 추측"""
    col_map = {}
    for i, cell in enumerate(header_row):
        if not cell:
            continue
        cl = str(cell).lower().strip()
        if cl in ('address', 'addr', '주소', 'offset') or \
           ('addr' in cl and 'register' not in cl and 'reg.' not in cl):
            col_map.setdefault('addr', i)
        elif cl.startswith('reg.') or cl == 'reg.addr':
            # EKOS: reg.addr(30041)가 실제 Modbus 주소, ADDRESS는 오프셋
            # reg.addr을 우선 주소로 사용
            col_map['addr'] = i  # 덮어쓰기 (ADDRESS보다 우선)
        elif any(k in cl for k in ['name', 'definition', '이름', '항목', 'parameter', 'description',
                                    'signal name', 'signalname']):
            col_map.setdefault('name', i)
        elif cl == 'field' or cl == '필드':
            col_map.setdefault('name', i)
        elif any(k in cl for k in ['data type', 'datatype', '데이터', '타입']):
            col_map.setdefault('type', i)
        elif cl == 'type' or cl == 'format':
            col_map.setdefault('type', i)
        elif any(k in cl for k in ['unit', '단위']):
            col_map.setdefault('unit', i)
        elif any(k in cl for k in ['scale', '배율', 'factor', 'gain']):
            col_map.setdefault('scale', i)
        elif any(k in cl for k in ['r/w', 'access', '읽기', 'permission', '속성']):
            col_map.setdefault('rw', i)
        elif any(k in cl for k in ['remark', 'comment', '비고', 'note', '설명']):
            col_map.setdefault('comment', i)
        elif ('register' in cl and ('number' in cl or 'num' in cl or 'count' in cl)) or \
             cl in ('numberofregister', 'numberofreg', 'regs', 'reg count'):
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

    _RANGE_ADDR_RE = re.compile(r'^(\d{4,5})\s*[-–~]\s*(\d{4,5})$')
    _RANGE_HEX_RE = re.compile(r'^(0x[0-9A-Fa-f]{4})\s*[-–~]\s*(0x[0-9A-Fa-f]{4})$', re.I)

    addr = None
    addr_raw = ''

    def _try_parse_addr(raw: str):
        raw = raw.strip()
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

    dtype = ''
    type_idx = col_map.get('type')
    if type_idx is not None and type_idx < len(row):
        m = _TYPE_RE.search(str(row[type_idx]))
        if m:
            dtype = m.group(1).upper()
    if not dtype:
        for cell in row:
            m = _TYPE_RE.search(str(cell))
            if m:
                dtype = m.group(1).upper()
                break
    dtype = dtype.upper()
    for old, new in [('INT16', 'S16'), ('UINT16', 'U16'), ('INT32', 'S32'), ('UINT32', 'U32'),
                     ('I16', 'S16'), ('I32', 'S32'), ('STR', 'STRING'),
                     ('BITFIELD16', 'U16'), ('BITFIELD32', 'U32')]:
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
            rw = m.group(1).upper().replace('READ', 'RO').replace('WRITE', 'WO')
    if not rw:
        for cell in row:
            m = _RW_RE.search(str(cell))
            if m:
                rw = m.group(1).upper().replace('READ', 'RO').replace('WRITE', 'WO')
                break

    comment = ''
    comment_idx = col_map.get('comment')
    if comment_idx is not None and comment_idx < len(row):
        comment = str(row[comment_idx]).strip()
        if comment in ('None', '-'):
            comment = ''

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
            ['sn', 'pf', 'pv', 'dc', 'ac', 'bus', 'ia', 'ib', 'ic', 'ua', 'ub', 'uc']):
        return False
    return True


def extract_registers_from_tables(tables: List[List[list]]) -> List[RegisterRow]:
    """모든 테이블에서 레지스터 행 추출"""
    registers = []
    seen_addrs = set()
    prev_col_map = {}  # 이전 테이블에서 감지한 col_map 유지
    for table in tables:
        if not table or len(table) < 1:
            continue
        table = _clean_table(table)
        first_has_addr = any(
            str(c).strip().startswith('0x') or str(c).strip().startswith('0X') or
            (re.match(r'^\d{3,5}$', str(c).strip()) and 0 <= int(str(c).strip()) <= 65535)
            for c in table[0] if c)
        if first_has_addr:
            data_rows = table
            col_map = _detect_table_columns([], data_rows)
            # 헤더 없는 테이블 — 이전 col_map이 더 풍부하면 사용
            if prev_col_map and len(col_map) < len(prev_col_map):
                col_map = dict(prev_col_map)
        else:
            data_rows = table[1:]
            col_map = _detect_table_columns(table[0], data_rows)
            if col_map:
                prev_col_map = dict(col_map)
        for row in data_rows:
            reg = _parse_register_row(row, col_map)
            if reg and reg.address not in seen_addrs:
                # V2: 유효하지 않은 이름 필터 (모델명 테이블, 숫자값 등)
                if not _is_valid_register_name(reg.definition):
                    continue
                seen_addrs.add(reg.address)
                registers.append(reg)
    return registers


# ─── H01 필드 매핑 ──────────────────────────────────────────────────────────

def assign_h01_field(reg: RegisterRow, synonym_db: dict,
                     ref_patterns: dict = None) -> str:
    """레지스터에 대응하는 H01 필드명 추정 (V2)"""
    defn_lower = reg.definition.lower().replace('_', ' ')
    category = getattr(reg, 'category', '')

    # V2: INFO/ALARM 카테고리는 H01 모니터링 필드가 아님 — 특정 키워드만 매칭
    if category == 'INFO':
        # INFO에서 H01과 겹치는 필드만 매핑
        if any(k in defn_lower for k in ['total energy', 'cumulative energy', 'total power yields',
                                          'total energy yield', 'energy yield',
                                          '누적발전량', '누적 발전량', '적산전력량',
                                          'total generation energy']):
            return 'cumulative_energy'
        if any(k in defn_lower for k in ['daily energy', 'today energy', 'daily power yields',
                                          '일발전량', '일 발전량', '금일발전량']):
            return 'daily_energy'
        if any(k in defn_lower for k in ['inner_temp', 'inner temp', 'internal temp',
                                          'module temp', 'inverter temp']):
            return 'temperature'
        return ''  # 나머지 INFO는 h01_field 없음

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
                                      'input power', 'pac', 'output power',
                                      'inverter current output',
                                      '태양전지 전력', '태양전지전력']):
        return 'pv_power'
    defn_nospace = defn_lower.replace(' ', '')
    # 'total generation'만으로는 안 됨 — 'total generation time'과 충돌
    # 'energy', 'yield', 'power' 키워드 필수
    if any(k in defn_lower for k in ['total energy', 'cumulative energy', 'total power yields',
                                      'lifetime energy', 'accumulated energy',
                                      'total power generation', 'total powergeneration',
                                      'total energy yield', 'energy yield',
                                      '누적발전량', '누적 발전량', '적산전력량',
                                      'total generation energy']) or \
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

    status_reg = _find_matched_reg(categorized, 'inverter_status', cat='STATUS')
    if not status_reg and categorized.get('STATUS'):
        status_reg = categorized['STATUS'][0]
    rows.append(_make_pdf_match_row('status', status_reg, 'Work State 미발견'))

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
    """PDF 매칭 행 생성 — scale은 PDF 원문 그대로, 단위 변환은 Note에만"""
    if reg:
        _, unit_desc = _get_unit_scale(reg.unit, h01_field)
        return {
            'field': h01_field, 'source': 'PDF', 'status': 'O',
            'address': reg.address_hex, 'definition': reg.definition,
            'type': reg.data_type, 'unit': reg.unit or '',
            'scale': reg.scale or '',  # PDF 원문 스케일 그대로
            'note': unit_desc,  # 단위 변환 참고만
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

    if ext == '.pdf':
        pages = extract_pdf_text_and_tables(input_path)
        log(f'  PDF {len(pages)}페이지 추출')

        _3X_START = ['running information variable address', 'running information']
        _4X_START = ['parameter setting address definition', 'parameter setting']

        section_3x_start = None
        section_4x_start = None
        for p in pages:
            page_text = p['text'].lower()
            pnum = p['page']
            if section_3x_start is None and any(m in page_text for m in _3X_START):
                section_3x_start = pnum
            if section_4x_start is None and any(m in page_text for m in _4X_START):
                if pnum != section_3x_start:
                    section_4x_start = pnum

        tables_3x, tables_4x, tables_other = [], [], []
        for p in pages:
            pnum = p['page']
            in_3x = (section_3x_start is not None and pnum >= section_3x_start and
                     (section_4x_start is None or pnum < section_4x_start))
            in_4x = (section_4x_start is not None and pnum >= section_4x_start)
            for tab in p['tables']:
                if in_4x:
                    tables_4x.append(tab)
                elif in_3x:
                    tables_3x.append(tab)
                else:
                    tables_other.append(tab)

        all_tables = tables_3x + tables_other + tables_4x
        log(f'  3X(Read-Only): {len(tables_3x)}개 (page {section_3x_start}~), '
            f'4X(Holding): {len(tables_4x)}개 (page {section_4x_start}~), '
            f'기타: {len(tables_other)}개')

    elif ext in ('.xlsx', '.xls'):
        sheets = extract_excel_sheets(input_path)
        log(f'  Excel {len(sheets)}시트 추출')
        for sname, rows in sheets.items():
            if rows:
                all_tables.append(rows)
    else:
        raise ValueError(f'지원하지 않는 파일 형식: {ext}')

    log('레지스터 테이블 파싱...')
    registers = extract_registers_from_tables(all_tables)
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
        if name in seen_names:
            registers[i] = None
        else:
            seen_names[name] = i
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

    log('카테고리 분류 중 (MAPPING_RULES_V2)...')
    categorized = {cat: [] for cat in
                   ['INFO', 'MONITORING', 'STATUS', 'ALARM',
                    'DER_CONTROL', 'DER_MONITOR', 'IV_SCAN', 'REVIEW']}
    excluded = []

    for reg in registers:
        cat, reason = classify_register_with_rules(
            reg, synonym_db, review_history, ref_patterns,
            device_type, all_regs=registers)
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
    info_cols = ['No', 'Definition', 'Address', 'Type', 'Unit/Scale', 'R/W', 'H01 Field', 'Comment']
    _write_header(ws, info_cols, info_start + 1)

    info_regs = sorted(categorized.get('INFO', []),
                       key=lambda r: (r.address if isinstance(r.address, int) else 0))
    for i, reg in enumerate(info_regs, start=1):
        su = f'{reg.unit} {reg.scale}'.strip() if reg.unit or reg.scale else ''
        vals = [i, reg.definition, reg.address_hex, reg.data_type, su,
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
        vals = [i, reg.definition, reg.address_hex, reg.data_type, su,
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
        vals = [i, reg.definition, reg.address_hex, reg.data_type, su,
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
    }


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
