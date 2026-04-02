# -*- coding: utf-8 -*-
"""
Stage 1 — PDF/Excel → Stage 1 Excel 레지스터맵 추출

PDF(PyMuPDF)나 Excel(openpyxl)에서 Modbus 레지스터 테이블을 추출하고,
synonym_db / review_history / 레퍼런스 패턴을 이용하여 카테고리 분류 및 H01 매칭을 수행한다.
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
    to_upper_snake, parse_address, match_synonym, match_synonym_fuzzy,
    detect_channel_number, detect_channel_from_ref,
    get_ref_name_by_addr, get_h01_field_from_ref,
    load_synonym_db, load_review_history, load_reference_patterns,
    get_openpyxl, ProgressCallback,
)
from .rules import (
    classify_register_with_rules, detect_iv_scan_support,
    get_valid_categories, distribute_alarms,
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
    """openpyxl로 Excel 시트별 행 추출"""
    openpyxl = get_openpyxl()
    wb = openpyxl.load_workbook(excel_path, data_only=True)
    result = {}
    for name in wb.sheetnames:
        ws = wb[name]
        rows = []
        for row in ws.iter_rows(values_only=True):
            if any(c is not None for c in row):
                rows.append([str(c) if c is not None else '' for c in row])
        result[name] = rows
    wb.close()
    return result


# ─── 레지스터 테이블 행 파싱 ──────────────────────────────────────────────────

# 주소+이름+타입 패턴
_ADDR_RE = re.compile(r'(?:0x|0X)?([0-9A-Fa-f]{4})[Hh]?')
_TYPE_RE = re.compile(r'\b(U16|S16|U32|S32|I16|I32|INT16|UINT16|INT32|UINT32|FLOAT32|ASCII|STRING|STR|Bitfield16|Bitfield32)\b', re.I)
_RW_RE   = re.compile(r'\b(R/?W|RO|WO|Read|Write|R/W)\b', re.I)
_SCALE_RE = re.compile(r'(?:scale|factor|×|x)\s*[=:]?\s*([\d.]+)', re.I)
_UNIT_RE = re.compile(r'\b(V|A|W|kW|VA|kVA|VAr|kVAr|Hz|°C|℃|Wh|kWh|MWh|%)\b')


def _detect_table_columns(header_row: list, data_rows: list = None) -> dict:
    """테이블 헤더에서 컬럼 인덱스 추측. data_rows로 보정."""
    col_map = {}
    for i, cell in enumerate(header_row):
        if not cell:
            continue
        cl = str(cell).lower().strip()
        # 'address'는 매칭하되 'register number'는 제외
        if cl in ('address', 'addr', '주소', 'offset') or \
           ('addr' in cl and 'register' not in cl):
            col_map.setdefault('addr', i)
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
        elif any(k in cl for k in ['r/w', 'access', '읽기', 'permission']):
            col_map.setdefault('rw', i)
        elif any(k in cl for k in ['remark', 'comment', '비고', 'note', '설명']):
            col_map.setdefault('comment', i)
        elif ('register' in cl and ('number' in cl or 'num' in cl or 'count' in cl)) or \
             cl in ('numberofregister', 'numberofreg', 'regs', 'reg count'):
            col_map.setdefault('regs', i)

    # data_rows에서 0x 패턴으로 주소 컬럼 보정
    if data_rows and 'addr' not in col_map:
        for row in data_rows[:5]:
            for i, cell in enumerate(row):
                if cell and re.match(r'0x[0-9A-Fa-f]{4}', str(cell).strip()):
                    col_map['addr'] = i
                    break
            if 'addr' in col_map:
                break

    return col_map


def _parse_register_row(row: list, col_map: dict) -> Optional[RegisterRow]:
    """테이블 행 → RegisterRow"""
    if not row:
        return None

    # 주소 추출 — 0x 패턴 (0x0~0xFFFF) 또는 decimal (0~65535)
    # 범위 주소 "5004 - 5005", "5004-5005", "5004~5005" → 시작 주소 사용
    _RANGE_ADDR_RE = re.compile(r'^(\d{4,5})\s*[-–~]\s*(\d{4,5})$')
    _RANGE_HEX_RE = re.compile(r'^(0x[0-9A-Fa-f]{4})\s*[-–~]\s*(0x[0-9A-Fa-f]{4})$', re.I)

    addr = None
    addr_raw = ''

    def _try_parse_addr(raw: str):
        """주소 문자열 파싱 (단일 또는 범위)"""
        raw = raw.strip()
        if not raw:
            return None
        # 0x hex
        if raw.startswith('0x') or raw.startswith('0X'):
            return parse_address(raw)
        # hex 범위 "0x1388 - 0x1389"
        m = _RANGE_HEX_RE.match(raw)
        if m:
            return parse_address(m.group(1))
        # 단일 decimal "5000"
        if re.match(r'^\d{1,5}$', raw) and 0 <= int(raw) <= 65535:
            return int(raw)
        # decimal 범위 "5004 - 5005"
        m = _RANGE_ADDR_RE.match(raw)
        if m:
            return int(m.group(1))
        return None

    # 1) col_map['addr'] 위치에서 시도
    addr_idx = col_map.get('addr')
    if addr_idx is not None and addr_idx < len(row):
        addr_raw = str(row[addr_idx]).strip() if row[addr_idx] else ''
        addr = _try_parse_addr(addr_raw)

    # 2) 실패하면 0x 패턴이 있는 아무 셀에서 찾기
    if addr is None:
        for i, cell in enumerate(row):
            c = str(cell).strip() if cell else ''
            parsed = _try_parse_addr(c)
            if parsed is not None:
                addr = parsed
                addr_raw = c
                if 'addr' not in col_map:
                    col_map['addr'] = i
                break

    if addr is None:
        return None

    # 이름 추출
    name_idx = col_map.get('name')
    if name_idx is not None and name_idx < len(row):
        name = str(row[name_idx]).strip()
    else:
        # 이름 컬럼이 없으면 주소 옆 셀에서 찾기
        name = ''
        for i, cell in enumerate(row):
            if i == addr_idx:
                continue
            c = str(cell).strip()
            if c and not _ADDR_RE.fullmatch(c) and len(c) > 2:
                name = c
                break
    if not name:
        return None

    # 데이터 타입
    dtype = ''
    type_idx = col_map.get('type')
    if type_idx is not None and type_idx < len(row):
        m = _TYPE_RE.search(str(row[type_idx]))
        if m:
            dtype = m.group(1).upper()
    if not dtype:
        # 전체 행에서 탐색
        for cell in row:
            m = _TYPE_RE.search(str(cell))
            if m:
                dtype = m.group(1).upper()
                break
    # 타입 정규화
    dtype = dtype.upper()
    for old, new in [('INT16', 'S16'), ('UINT16', 'U16'), ('INT32', 'S32'), ('UINT32', 'U32'),
                     ('I16', 'S16'), ('I32', 'S32'), ('STR', 'STRING'),
                     ('BITFIELD16', 'U16'), ('BITFIELD32', 'U32')]:
        dtype = dtype.replace(old, new)

    # 단위
    unit = ''
    unit_idx = col_map.get('unit')
    if unit_idx is not None and unit_idx < len(row):
        m = _UNIT_RE.search(str(row[unit_idx]))
        if m:
            unit = m.group(1)
    if not unit:
        m = _UNIT_RE.search(name)
        if m:
            unit = m.group(1)

    # 스케일
    scale = ''
    scale_idx = col_map.get('scale')
    if scale_idx is not None and scale_idx < len(row):
        s = str(row[scale_idx]).strip()
        if s and s not in ('', 'None', '-'):
            scale = s
    if not scale:
        m = _SCALE_RE.search(' '.join(str(c) for c in row))
        if m:
            scale = m.group(1)

    # R/W
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

    # 코멘트
    comment = ''
    comment_idx = col_map.get('comment')
    if comment_idx is not None and comment_idx < len(row):
        comment = str(row[comment_idx]).strip()
        if comment in ('None', '-'):
            comment = ''

    # 레지스터 수
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
    """셀 내용에서 줄바꿈 제거 + 정리"""
    if cell is None:
        return ''
    return re.sub(r'\s*\n\s*', '', str(cell)).strip()


def _clean_table(table: List[list]) -> List[list]:
    """테이블 전체 셀에서 줄바꿈 제거"""
    return [[_clean_cell(c) for c in row] for row in table]


def extract_registers_from_tables(tables: List[List[list]]) -> List[RegisterRow]:
    """모든 테이블에서 레지스터 행 추출"""
    registers = []
    seen_addrs = set()
    for table in tables:
        if not table or len(table) < 1:
            continue
        # 셀 줄바꿈 제거 (화웨이 PDF 등)
        table = _clean_table(table)
        # 첫 행에 0x 주소 또는 5자리 숫자가 있으면 헤더 없는 테이블
        first_has_addr = any(
            str(c).strip().startswith('0x') or str(c).strip().startswith('0X') or
            (re.match(r'^\d{4,5}$', str(c).strip()) and 0 <= int(str(c).strip()) <= 65535)
            for c in table[0] if c)
        if first_has_addr:
            data_rows = table
            col_map = _detect_table_columns([], data_rows)
        else:
            data_rows = table[1:]
            col_map = _detect_table_columns(table[0], data_rows)
        for row in data_rows:
            reg = _parse_register_row(row, col_map)
            if reg and reg.address not in seen_addrs:
                seen_addrs.add(reg.address)
                registers.append(reg)
    return registers


# ─── 카테고리 분류 ───────────────────────────────────────────────────────────

# 제외 키워드 (STAGE1_RULES §6)
_EXCLUDE_KEYWORDS = [
    'baud', 'baudrate', 'slave id', 'slave address', 'communication',
    'comm setting', 'rs485', 'modbus addr', 'protocol version',
]

# REMS 제외 키워드
_REMS_KEYWORDS = ['rems', 'remote monitoring']

# IV Scan 키워드
_IV_KEYWORDS = ['iv scan', 'iv curve', 'i-v scan', 'iv test', 'iv start',
                'iv status', 'iv result', 'iv point', 'iv data']


def classify_register(reg: RegisterRow, synonym_db: dict,
                      review_history: dict,
                      ref_patterns: dict,
                      device_type: str = 'inverter') -> str:
    """
    레지스터를 카테고리로 분류
    Returns: 카테고리 문자열
    """
    defn = reg.definition
    defn_lower = defn.lower()
    defn_upper = to_upper_snake(defn)
    addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)

    # 제외 항목
    for kw in _EXCLUDE_KEYWORDS:
        if kw in defn_lower:
            return 'EXCLUDE'
    for kw in _REMS_KEYWORDS:
        if kw in defn_lower:
            return 'EXCLUDE'

    # 1) 레퍼런스 패턴 기반 매칭 (주소로)
    if addr is not None:
        for proto, addr_map in ref_patterns.items():
            if addr in addr_map:
                ref_name = addr_map[addr]
                ref_upper = ref_name.upper()
                if 'DEA_' in ref_upper or 'DEA_' in ref_upper:
                    return 'DER_MONITOR' if device_type == 'inverter' else 'EXCLUDE'
                if 'DER_' in ref_upper or 'INVERTER_ON_OFF' in ref_upper:
                    return 'DER_CONTROL' if device_type == 'inverter' else 'EXCLUDE'
                if 'IV_' in ref_upper:
                    return 'IV_SCAN' if device_type == 'inverter' else 'EXCLUDE'
                if 'ERROR_CODE' in ref_upper or 'ALARM' in ref_upper:
                    return 'ALARM'
                if 'INVERTER_MODE' in ref_upper or 'STATUS' in ref_upper:
                    return 'STATUS'
                if any(k in ref_upper for k in ['MODEL', 'SERIAL', 'FIRMWARE', 'NOMINAL', 'MPPT_COUNT', 'PHASE']):
                    return 'INFO'
                return 'MONITORING'

    # 2) synonym_db 매칭
    syn_match = match_synonym(defn, synonym_db)
    if syn_match:
        cat = syn_match['category']
        if cat in ('DER_CONTROL', 'DER_MONITOR', 'IV_SCAN') and device_type != 'inverter':
            return 'EXCLUDE'
        return cat

    # 3) 키워드 기반 분류
    # INFO
    if any(k in defn_lower for k in ['model', 'serial', 'firmware', 'rated', 'nominal',
                                      'version', '모델', '시리얼', '펌웨어', '정격',
                                      'device type code', 'type code', 'output type',
                                      'total running time', 'daily running time',
                                      'internal temperature', 'inner temperature',
                                      'apparent power', 'present country', 'country code',
                                      'insulation resistance', 'bus voltage',
                                      'total power yields', 'daily power yields',
                                      'monthly power yields', 'total energy',
                                      'daily energy', 'today energy',
                                      'nominal reactive', 'nominal active']):
        return 'INFO'

    # STATUS
    if any(k in defn_lower for k in ['status', 'state', 'mode', 'running',
                                      '상태', '운전', '동작']):
        return 'STATUS'

    # ALARM
    if any(k in defn_lower for k in ['alarm', 'fault', 'error', 'warning', 'protection',
                                      '알람', '고장', '경보', '오류']):
        return 'ALARM'

    # IV Scan (인버터만)
    if device_type == 'inverter':
        for kw in _IV_KEYWORDS:
            if kw in defn_lower:
                return 'IV_SCAN'

    # DER 키워드 (인버터만)
    if device_type == 'inverter':
        if reg.rw in ('RW', 'WO', 'R/W'):
            if any(k in defn_lower for k in ['power limit', 'power factor set',
                                              'reactive power set', 'on/off', 'on off',
                                              'curtailment', 'derate', 'operation mode set',
                                              '출력제한', '역률설정', '무효전력설정']):
                return 'DER_CONTROL'

    # MPPT/String → MONITORING
    ch = detect_channel_number(defn)
    if ch:
        return 'MONITORING'

    # 모니터링 키워드
    if any(k in defn_lower for k in ['voltage', 'current', 'power', 'energy', 'frequency',
                                      'temperature', 'factor', '전압', '전류', '전력',
                                      '발전량', '온도', '주파수', '역률']):
        return 'MONITORING'

    # 4) 퍼지 매칭
    fuzzy = match_synonym_fuzzy(defn, synonym_db, threshold=0.6)
    if fuzzy:
        cat = fuzzy['category']
        if cat in ('DER_CONTROL', 'DER_MONITOR', 'IV_SCAN') and device_type != 'inverter':
            return 'EXCLUDE'
        return cat

    # 5) review_history에서 동일 패턴 확인
    for item in review_history.get('approved', []):
        if item.get('definition', '').upper() == defn_upper:
            verdict = item.get('verdict', '')
            if verdict == 'DELETE':
                return 'EXCLUDE'
            if verdict.startswith('MOVE:'):
                return verdict.replace('MOVE:', '')

    # 분류 불가 → REVIEW
    return 'REVIEW'


def assign_h01_field(reg: RegisterRow, synonym_db: dict,
                     ref_patterns: dict = None) -> str:
    """레지스터에 대응하는 H01 필드명 추정"""
    # 0) 채널 번호가 있으면 최우선 — PV{n}/MPPT{n}/STRING{n} → mppt{n}/string{n}
    ch = detect_channel_number(reg.definition)
    if ch:
        prefix, n = ch
        defn_lower = reg.definition.lower()
        if prefix == 'MPPT':
            if 'voltage' in defn_lower:
                return f'mppt{n}_voltage'
            if 'current' in defn_lower:
                return f'mppt{n}_current'
            if 'power' in defn_lower:
                return f'mppt{n}_power'
        elif prefix == 'STRING':
            if 'voltage' in defn_lower:
                return f'string{n}_voltage'
            if 'current' in defn_lower:
                return f'string{n}_current'

    # 1) synonym_db 정확 매칭
    syn_match = match_synonym(reg.definition, synonym_db)
    if syn_match and syn_match.get('h01_field'):
        return syn_match['h01_field']

    # 2) 주소 기반 레퍼런스 → h01_field
    if ref_patterns:
        addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)
        if addr is not None:
            h01 = get_h01_field_from_ref(addr, ref_patterns, synonym_db)
            if h01:
                return h01

    # 3) 퍼지 매칭
    fuzzy = match_synonym_fuzzy(reg.definition, synonym_db)
    if fuzzy and fuzzy.get('h01_field'):
        return fuzzy['h01_field']

    # 4) MPPT/String 패턴 (Step 0에서 못 잡힌 경우 — 한글 키워드 포함)
    ch = detect_channel_number(reg.definition)
    if ch:
        prefix, n = ch
        defn_lower = reg.definition.lower()
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
    return ''


# ─── Stage 1 메인 함수 ───────────────────────────────────────────────────────

def run_stage1(
    input_path: str,
    output_dir: str,
    device_type: str = 'inverter',
    progress: ProgressCallback = None,
) -> dict:
    """
    Stage 1 실행: PDF/Excel → Stage 1 Excel

    Args:
        input_path: PDF 또는 Excel 파일 경로
        output_dir: 출력 디렉토리
        device_type: 'inverter', 'relay', 'weather'
        progress: 진행 콜백 (message, level)

    Returns:
        {'output_path': str, 'counts': dict, 'meta': dict, 'review_count': int}
    """
    def log(msg, level='info'):
        if progress:
            progress(msg, level)

    openpyxl = get_openpyxl()
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    # ── Step 0: 레퍼런스 로딩 ──
    log('레퍼런스 로딩 중...')
    synonym_db = load_synonym_db()
    review_history = load_review_history()
    ref_patterns = load_reference_patterns()
    log(f'  synonym_db: {len(synonym_db.get("fields", {}))}개 필드')
    log(f'  review_history: {len(review_history.get("approved", []))}개 이력')
    log(f'  레퍼런스: {len(ref_patterns)}개 프로토콜')

    # ── Step 1: 입력 파일 읽기 ──
    ext = os.path.splitext(input_path)[1].lower()
    basename = os.path.splitext(os.path.basename(input_path))[0]

    log(f'입력 파일 읽기: {os.path.basename(input_path)}')
    all_tables = []

    if ext == '.pdf':
        pages = extract_pdf_text_and_tables(input_path)
        log(f'  PDF {len(pages)}페이지 추출')

        # ── 3X(Read-Only) / 4X(Holding) 섹션 감지 ──
        # 3X = Input Register (FC04, read-only, running information)
        # 4X = Holding Register (FC03/06/10, R/W, parameter setting)
        # 같은 레지스터 번호(5000+)를 사용하므로 섹션별로 분리 필요
        #
        # 전략: 페이지를 순회하며 "현재 섹션" 상태를 추적
        # 3X 섹션 시작 마커를 만나면 → 이후 페이지는 3X
        # 4X 섹션 시작 마커를 만나면 → 이후 페이지는 4X
        # (단, 같은 페이지에 두 마커 모두 있으면 섹션 전환 시점)
        _3X_START = [
            'running information variable address',
            'running information',
        ]
        _4X_START = [
            'parameter setting address definition',
            'parameter setting',
        ]

        # 1단계: 3X/4X 섹션 시작 페이지 감지
        section_3x_start = None
        section_4x_start = None
        for p in pages:
            page_text = p['text'].lower()
            pnum = p['page']
            if section_3x_start is None and any(m in page_text for m in _3X_START):
                section_3x_start = pnum
            if section_4x_start is None and any(m in page_text for m in _4X_START):
                # 3X와 같은 페이지면 건너뛰기 (개요 페이지)
                if pnum != section_3x_start:
                    section_4x_start = pnum

        tables_3x = []
        tables_4x = []
        tables_other = []

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

        # 3X 우선: 3X 테이블 먼저, 그 다음 기타, 4X는 마지막
        # extract_registers_from_tables의 seen_addrs에 의해
        # 3X에서 먼저 추출된 주소는 4X에서 무시됨
        all_tables = tables_3x + tables_other + tables_4x

        log(f'  3X(Read-Only) 테이블: {len(tables_3x)}개 (page {section_3x_start}~), '
            f'4X(Holding) 테이블: {len(tables_4x)}개 (page {section_4x_start}~), '
            f'기타: {len(tables_other)}개')

    elif ext in ('.xlsx', '.xls'):
        sheets = extract_excel_sheets(input_path)
        log(f'  Excel {len(sheets)}시트 추출')
        for sname, rows in sheets.items():
            if rows:
                all_tables.append(rows)
    else:
        raise ValueError(f'지원하지 않는 파일 형식: {ext}')

    # ── Step 2: 레지스터 추출 ──
    log('레지스터 테이블 파싱...')
    registers = extract_registers_from_tables(all_tables)
    log(f'  {len(registers)}개 레지스터 추출 (원본)')

    if not registers:
        raise ValueError('레지스터를 찾지 못했습니다. PDF/Excel 형식을 확인해주세요.')

    # ── Step 2.5: 제조사 조기 감지 + DER 고정 주소 제외 + 이름 정규화 ──
    manufacturer = basename.split('_')[0].split(' ')[0]
    log(f'  제조사 (파일명 기반): {manufacturer}')

    # DER-AVM 레지스터는 고정 주소맵으로 삽입하므로 PDF 파싱 결과에서 제거
    DER_FIXED_ADDRS = set()
    for dr in DER_CONTROL_REGS:
        DER_FIXED_ADDRS.add(dr['addr'])
    for dr in DER_MONITOR_REGS:
        DER_FIXED_ADDRS.add(dr['addr'])

    before_der_filter = len(registers)
    registers = [r for r in registers
                 if (r.address if isinstance(r.address, int) else parse_address(r.address))
                 not in DER_FIXED_ADDRS]
    der_removed = before_der_filter - len(registers)
    if der_removed:
        log(f'  DER 고정 주소 제외: {der_removed}개 (고정 주소맵으로 대체)')

    # 레퍼런스 기반 enrichment (같은 제조사 레퍼런스만 사용)
    if ref_patterns:
        # 제조사명으로 해당 레퍼런스만 필터
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
            log(f'  레퍼런스 enrichment: 해당 제조사({manufacturer}) 레퍼런스 없음 — 스킵')

    # synonym_db 기반 이름 정규화 (레퍼런스 없어도 동작)
    # 단, 채널 번호가 있는 레지스터(MPPT1, STRING2 등)는 정규화하지 않음
    # (synonym_db가 MPPT1 Voltage → MPPT_VOLTAGE로 채널 번호를 제거하면 중복 발생)
    normalized = 0
    for reg in registers:
        # 채널 번호 있으면 스킵
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

    # 중복 이름 제거
    seen_names = {}
    for i, reg in enumerate(registers):
        name = to_upper_snake(reg.definition)
        if name in seen_names:
            registers[i] = None
        else:
            seen_names[name] = i
    registers = [r for r in registers if r is not None]
    log(f'  중복 제거 후: {len(registers)}개')

    # ── Step 3: 제조사 정보 ──
    manufacturer = basename.split('_')[0].split(' ')[0]
    protocol_version = ''

    # PDF 전문에서 제조사/버전 탐색
    if ext == '.pdf':
        full_text = ' '.join(p['text'] for p in pages[:3])  # 처음 3페이지
        # 버전 패턴
        m = re.search(r'[Vv](\d+\.\d+(?:\.\d+)?)', full_text)
        if m:
            protocol_version = f'V{m.group(1)}'

    # MPPT / String 최대 채널 수 탐색 (PDF 이름 + 레퍼런스 이름 모두)
    max_mppt = 0
    max_string = 0
    iv_scan_supported = False

    for reg in registers:
        # 레지스터 이름에서 채널 감지
        ch = detect_channel_number(reg.definition)
        if not ch:
            # 주소로 레퍼런스에서 채널 감지
            addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)
            if addr is not None:
                ch = detect_channel_from_ref(addr, ref_patterns)
        if ch:
            prefix, n = ch
            if prefix == 'MPPT':
                max_mppt = max(max_mppt, n)
            elif prefix == 'STRING':
                max_string = max(max_string, n)
        # IV Scan 감지 (§5: rules.py 사용)

    iv_scan_supported = detect_iv_scan_support(registers, manufacturer)

    meta = {
        'manufacturer': manufacturer,
        'protocol_version': protocol_version,
        'device_type': device_type,
        'max_mppt': max_mppt,
        'max_string': max_string,
        'iv_scan': iv_scan_supported and device_type == 'inverter',
        'source_file': os.path.basename(input_path),
        'extracted_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'total_extracted': len(registers),
    }

    log(f'  제조사: {manufacturer}, MPPT: {max_mppt}, String: {max_string}')
    log(f'  IV Scan: {"Yes" if meta["iv_scan"] else "No"}')

    # ── Step 4: 카테고리 분류 ──
    log('카테고리 분류 중...')
    categorized = {cat: [] for cat in
                   ['INFO', 'MONITORING', 'STATUS', 'ALARM',
                    'DER_CONTROL', 'DER_MONITOR', 'IV_SCAN', 'REVIEW']}
    excluded = []

    for reg in registers:
        # §7: STAGE1_RULES 기반 분류 (rules.py)
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

    # 인버터: DER 표준 레지스터 — 고정 주소맵에서 항상 주입 (PDF 파싱 무관)
    if device_type == 'inverter':
        categorized['DER_CONTROL'] = []  # PDF 파싱 결과 제거, 고정맵만 사용
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

    # 비인버터: DER/IV 카테고리 제거
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

    # ── Step 5: Excel 생성 ──
    output_name = f'test_{basename}_stage1.xlsx'
    output_path = os.path.join(output_dir, output_name)
    log(f'Excel 생성: {output_name}')

    wb = openpyxl.Workbook()

    # --- SUMMARY 시트 ---
    ws = wb.active
    ws.title = 'SUMMARY'
    header_font = Font(bold=True, size=12)
    ws['A1'] = 'Stage 1 — 레지스터맵 추출 결과'
    ws['A1'].font = Font(bold=True, size=14)
    for i, (k, v) in enumerate([
        ('제조사', manufacturer),
        ('프로토콜 버전', protocol_version),
        ('설비 타입', device_type),
        ('MPPT 최대', max_mppt),
        ('String 최대', max_string),
        ('IV Scan', 'Yes' if meta['iv_scan'] else 'No'),
        ('원본 파일', os.path.basename(input_path)),
        ('추출 일시', meta['extracted_date']),
    ], start=3):
        ws[f'A{i}'] = k
        ws[f'A{i}'].font = Font(bold=True)
        ws[f'B{i}'] = str(v)

    row_n = 13
    ws[f'A{row_n}'] = '카테고리별 수량'
    ws[f'A{row_n}'].font = header_font
    for i, (cat, cnt) in enumerate(counts.items(), start=1):
        ws[f'A{row_n + i}'] = cat
        ws[f'B{row_n + i}'] = cnt
        ws[f'A{row_n + i}'].fill = PatternFill('solid', fgColor=CATEGORY_COLORS.get(cat, 'FFFFFF'))

    # --- META 시트 ---
    ws_meta = wb.create_sheet('META')
    for i, (k, v) in enumerate(meta.items(), start=1):
        ws_meta[f'A{i}'] = k
        ws_meta[f'B{i}'] = str(v)

    # --- 카테고리별 시트 ---
    thin_border = Border(
        left=Side(style='thin'), right=Side(style='thin'),
        top=Side(style='thin'), bottom=Side(style='thin'))

    cols = ['No', 'Definition', 'Address', 'Reg', 'Type', 'Unit/Scale', 'R/W',
            'Comment', 'H01 Field', 'Category']

    for cat in ['INFO', 'MONITORING', 'STATUS', 'ALARM',
                'DER_CONTROL', 'DER_MONITOR', 'IV_SCAN', 'REVIEW']:
        regs = categorized[cat]
        if not regs and cat not in ('REVIEW',):
            if device_type != 'inverter' and cat in ('DER_CONTROL', 'DER_MONITOR', 'IV_SCAN'):
                continue
            if not regs:
                continue

        ws_cat = wb.create_sheet(cat)
        cat_fill = PatternFill('solid', fgColor=CATEGORY_COLORS.get(cat, 'FFFFFF'))

        # 헤더
        review_cols = cols
        if cat == 'REVIEW':
            review_cols = cols + ['사유', '제안', '사용자 판정']
        for j, col_name in enumerate(review_cols, start=1):
            cell = ws_cat.cell(row=1, column=j, value=col_name)
            cell.font = Font(bold=True, color='FFFFFF')
            cell.fill = PatternFill('solid', fgColor='333333')
            cell.border = thin_border

        # 데이터
        for i, reg in enumerate(sorted(regs, key=lambda r: (r.address if isinstance(r.address, int) else 0)),
                                start=1):
            scale_unit = f'{reg.unit} {reg.scale}'.strip() if reg.unit or reg.scale else ''
            row_data = [
                i, reg.definition, reg.address_hex, reg.regs, reg.data_type,
                scale_unit, reg.rw, reg.comment, reg.h01_field, reg.category,
            ]
            if cat == 'REVIEW':
                row_data += [reg.review_reason, reg.review_suggestion, '']

            for j, val in enumerate(row_data, start=1):
                cell = ws_cat.cell(row=i + 1, column=j, value=val)
                cell.border = thin_border
                if j <= len(cols):
                    cell.fill = cat_fill

        # 열 너비
        ws_cat.column_dimensions['B'].width = 35
        ws_cat.column_dimensions['H'].width = 30
        if cat == 'REVIEW':
            ws_cat.column_dimensions['K'].width = 40
            ws_cat.column_dimensions['L'].width = 30
            ws_cat.column_dimensions['M'].width = 15

    # 저장
    wb.save(output_path)
    wb.close()
    log(f'Stage 1 완료: {output_name}', 'ok')

    return {
        'output_path': output_path,
        'output_name': output_name,
        'counts': counts,
        'meta': meta,
        'review_count': counts.get('REVIEW', 0),
    }
