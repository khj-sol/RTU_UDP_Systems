# -*- coding: utf-8 -*-
"""
Stage 1/2/3 Pipeline — 공유 유틸리티 및 상수
"""
import os
import re
import sys
import json
import glob
import importlib.util
from typing import Dict, List, Optional, Callable, Any

# 프로젝트 루트 경로
_HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(_HERE, '..', '..', '..'))
COMMON_DIR = os.path.join(PROJECT_ROOT, 'common')
MODEL_MAKER_DIR = os.path.join(PROJECT_ROOT, 'model_maker')
UDP_PROTOCOL_DIR = os.path.join(PROJECT_ROOT, 'UDP_SERVER_protocol')

SYNONYM_DB_PATH = os.path.join(MODEL_MAKER_DIR, 'synonym_db.json')
REVIEW_HISTORY_PATH = os.path.join(MODEL_MAKER_DIR, 'review_history.json')

# 콜백 타입
ProgressCallback = Optional[Callable[[str, str], None]]   # (message, level)


# ─── 레지스터 행 표준 구조 ────────────────────────────────────────────────────

class RegisterRow:
    """단일 레지스터 항목"""
    __slots__ = ('definition', 'address', 'address_hex', 'data_type', 'regs',
                 'unit', 'scale', 'rw', 'comment', 'category', 'h01_field',
                 'h01_match', 'der_match', 'review_reason', 'review_suggestion',
                 'user_verdict')

    def __init__(self, **kw):
        for s in self.__slots__:
            setattr(self, s, kw.get(s, ''))
        if self.address and not self.address_hex:
            try:
                self.address_hex = f'0x{int(self.address):04X}'
            except (ValueError, TypeError):
                self.address_hex = str(self.address)

    def to_dict(self) -> dict:
        return {s: getattr(self, s) for s in self.__slots__}

    @classmethod
    def from_dict(cls, d: dict) -> 'RegisterRow':
        return cls(**{k: v for k, v in d.items() if k in cls.__slots__})


# ─── InverterMode 표준 정의 ─────────────────────────────────────────────────

INVERTER_MODES = [
    (0x00, 'INITIAL',  'Initial mode'),
    (0x01, 'STANDBY',  'Standby mode'),
    (0x03, 'ON_GRID',  'On-Grid mode'),
    (0x04, 'OFF_GRID', 'Off-Grid mode'),
    (0x05, 'FAULT',    'Fault mode'),
    (0x09, 'SHUTDOWN', 'Shutdown mode'),
]

# ─── 표준 DER-AVM 레지스터 ──────────────────────────────────────────────────

DER_CONTROL_REGS = [
    {'addr': 0x07D0, 'name': 'DER_POWER_FACTOR_SET',        'type': 'S16', 'scale': '0.001', 'rw': 'RW', 'desc': 'Power factor [-1000,-800],[800,1000]'},
    {'addr': 0x07D1, 'name': 'DER_ACTION_MODE',             'type': 'U16', 'scale': '1',     'rw': 'RW', 'desc': 'Action mode: 0=Self, 2=DER-AVM, 5=Q(V)'},
    {'addr': 0x07D2, 'name': 'DER_REACTIVE_POWER_PCT',      'type': 'S16', 'scale': '0.1',   'rw': 'RW', 'desc': 'Reactive power % [-484,484]'},
    {'addr': 0x07D3, 'name': 'DER_ACTIVE_POWER_PCT',        'type': 'U16', 'scale': '0.1',   'rw': 'RW', 'desc': 'Active power % [0,1100]'},
    {'addr': 0x0834, 'name': 'INVERTER_ON_OFF',             'type': 'U16', 'scale': '1',     'rw': 'RW', 'desc': 'On/Off: 0=ON, 1=OFF'},
    {'addr': 0x0835, 'name': 'CLEAR_PV_INSULATION_WARNING', 'type': 'U16', 'scale': '1',     'rw': 'WO', 'desc': 'Clear PV insulation warning'},
    {'addr': 0x6001, 'name': 'INVERTER_CONTROL',            'type': 'U16', 'scale': '1',     'rw': 'WO', 'desc': 'Inverter: 0=Power on, 1=Shut down'},
    {'addr': 0x600D, 'name': 'IV_CURVE_SCAN',               'type': 'U16', 'scale': '1',     'rw': 'RW', 'desc': 'IV scan: W 0=Stop,1=Start; R 0=Idle,1=Running,2=Finished'},
    {'addr': 0x600F, 'name': 'POWER_FACTOR_DYNAMIC',        'type': 'S16', 'scale': '0.001', 'rw': 'RW', 'desc': 'Dynamic power factor'},
    {'addr': 0x6010, 'name': 'REACTIVE_POWER_DYNAMIC',      'type': 'S16', 'scale': '0.01',  'rw': 'RW', 'desc': 'Dynamic reactive power (%)'},
    {'addr': 0x3005, 'name': 'POWER_DERATING_PCT',          'type': 'U16', 'scale': '1',     'rw': 'RW', 'desc': 'Active power derating [0-110]%'},
]

DER_MONITOR_REGS = [
    {'addr': 0x03E8, 'name': 'DEA_L1_CURRENT_LOW',           'type': 'S32', 'scale': '0.1', 'unit': 'A',   'desc': 'DEA L1 current (low)'},
    {'addr': 0x03E9, 'name': 'DEA_L1_CURRENT_HIGH',          'type': 'S32', 'desc': '(high)'},
    {'addr': 0x03EA, 'name': 'DEA_L2_CURRENT_LOW',           'type': 'S32', 'scale': '0.1', 'unit': 'A',   'desc': 'DEA L2 current (low)'},
    {'addr': 0x03EB, 'name': 'DEA_L2_CURRENT_HIGH',          'type': 'S32', 'desc': '(high)'},
    {'addr': 0x03EC, 'name': 'DEA_L3_CURRENT_LOW',           'type': 'S32', 'scale': '0.1', 'unit': 'A',   'desc': 'DEA L3 current (low)'},
    {'addr': 0x03ED, 'name': 'DEA_L3_CURRENT_HIGH',          'type': 'S32', 'desc': '(high)'},
    {'addr': 0x03EE, 'name': 'DEA_L1_VOLTAGE_LOW',           'type': 'S32', 'scale': '0.1', 'unit': 'V',   'desc': 'DEA L1 voltage (low)'},
    {'addr': 0x03EF, 'name': 'DEA_L1_VOLTAGE_HIGH',          'type': 'S32', 'desc': '(high)'},
    {'addr': 0x03F0, 'name': 'DEA_L2_VOLTAGE_LOW',           'type': 'S32', 'scale': '0.1', 'unit': 'V',   'desc': 'DEA L2 voltage (low)'},
    {'addr': 0x03F1, 'name': 'DEA_L2_VOLTAGE_HIGH',          'type': 'S32', 'desc': '(high)'},
    {'addr': 0x03F2, 'name': 'DEA_L3_VOLTAGE_LOW',           'type': 'S32', 'scale': '0.1', 'unit': 'V',   'desc': 'DEA L3 voltage (low)'},
    {'addr': 0x03F3, 'name': 'DEA_L3_VOLTAGE_HIGH',          'type': 'S32', 'desc': '(high)'},
    {'addr': 0x03F4, 'name': 'DEA_TOTAL_ACTIVE_POWER_LOW',   'type': 'S32', 'scale': '0.1', 'unit': 'kW',  'desc': 'DEA active power (low)'},
    {'addr': 0x03F5, 'name': 'DEA_TOTAL_ACTIVE_POWER_HIGH',  'type': 'S32', 'desc': '(high)'},
    {'addr': 0x03F6, 'name': 'DEA_TOTAL_REACTIVE_POWER_LOW', 'type': 'S32', 'scale': '1',   'unit': 'Var', 'desc': 'DEA reactive power (low)'},
    {'addr': 0x03F7, 'name': 'DEA_TOTAL_REACTIVE_POWER_HIGH','type': 'S32', 'desc': '(high)'},
    {'addr': 0x03F8, 'name': 'DEA_POWER_FACTOR_LOW',         'type': 'S32', 'scale': '0.001', 'desc': 'DEA power factor (low)'},
    {'addr': 0x03F9, 'name': 'DEA_POWER_FACTOR_HIGH',        'type': 'S32', 'desc': '(high)'},
    {'addr': 0x03FA, 'name': 'DEA_FREQUENCY_LOW',            'type': 'S32', 'scale': '0.1', 'unit': 'Hz',  'desc': 'DEA frequency (low)'},
    {'addr': 0x03FB, 'name': 'DEA_FREQUENCY_HIGH',           'type': 'S32', 'desc': '(high)'},
    {'addr': 0x03FC, 'name': 'DEA_STATUS_FLAG_LOW',          'type': 'S32', 'scale': '1',   'desc': 'DEA status flag (low)'},
    {'addr': 0x03FD, 'name': 'DEA_STATUS_FLAG_HIGH',         'type': 'S32', 'desc': '(high)'},
]


# ─── 유틸리티 ─────────────────────────────────────────────────────────────────

def to_upper_snake(name: str) -> str:
    """필드명 → UPPER_SNAKE_CASE"""
    s = re.sub(r'[()（）\[\]【】]', '', str(name))
    s = re.sub(r'[\s\-./·:]+', '_', s)
    s = re.sub(r'([a-z])([A-Z])', r'\1_\2', s)
    s = re.sub(r'[^A-Za-z0-9_]', '', s)
    s = re.sub(r'_+', '_', s).strip('_')
    result = s.upper()
    if result and result[0].isdigit():
        result = 'REG_' + result
    return result


def parse_address(raw) -> Optional[int]:
    """다양한 형식의 주소를 정수로 변환 (0x1001, 4097, '1001H' 등)"""
    if isinstance(raw, (int, float)):
        v = int(raw)
        return v if 0 <= v <= 0xFFFF else None
    s = str(raw).strip()
    if not s:
        return None
    # 0x prefix
    m = re.match(r'(?:0x|0X)([0-9A-Fa-f]+)', s)
    if m:
        return int(m.group(1), 16)
    # H suffix
    m = re.match(r'([0-9A-Fa-f]+)[Hh]$', s)
    if m:
        return int(m.group(1), 16)
    # Pure hex (4+ digits, has A-F)
    if re.match(r'^[0-9A-Fa-f]{4,}$', s) and re.search(r'[A-Fa-f]', s):
        return int(s, 16)
    # Decimal
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


# ─── 레퍼런스 로더 ──────────────────────────────────────────────────────────

def load_synonym_db() -> dict:
    """synonym_db.json 로드"""
    if not os.path.exists(SYNONYM_DB_PATH):
        return {'fields': {}}
    with open(SYNONYM_DB_PATH, encoding='utf-8') as f:
        return json.load(f)


def save_synonym_db(db: dict):
    """synonym_db.json 저장"""
    with open(SYNONYM_DB_PATH, 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


def load_review_history() -> dict:
    """review_history.json 로드"""
    if not os.path.exists(REVIEW_HISTORY_PATH):
        return {'approved': [], 'stats': {'total_reviewed': 0, 'auto_applied': 0}}
    with open(REVIEW_HISTORY_PATH, encoding='utf-8') as f:
        return json.load(f)


def save_review_history(history: dict):
    """review_history.json 저장"""
    with open(REVIEW_HISTORY_PATH, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def load_reference_patterns() -> Dict[str, Dict[int, str]]:
    """
    common/*_registers.py에서 RegisterMap 속성 → 주소 매핑 수집
    Returns: {protocol_name: {address: attr_name}}
    """
    patterns = {}
    py_files = glob.glob(os.path.join(COMMON_DIR, '*_registers.py'))
    py_files += glob.glob(os.path.join(COMMON_DIR, '*_mm_registers.py'))
    for fpath in set(py_files):
        fname = os.path.basename(fpath)
        proto = fname.replace('_mm_registers.py', '').replace('_registers.py', '')
        try:
            spec = importlib.util.spec_from_file_location(f'ref_{proto}', fpath)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            rm = getattr(mod, 'RegisterMap', None)
            if rm is None:
                continue
            addr_map = {}
            # 별칭 감지: 같은 주소를 가리키는 속성이 여러 개면 원본(비별칭) 우선
            # 우선순위: L1/L2/L3 > R/S/T, 언더스코어 포함 긴 이름 > 짧은 별칭
            alias_prefixes = ('R_', 'S_', 'T_', 'R_PHASE', 'S_PHASE', 'T_PHASE',
                              'FIRMWARE_VERSION', 'AC_POWER', 'PV_POWER', 'FREQUENCY',
                              'TOTAL_ENERGY', 'GRID_POWER', 'ACTION_MODE',
                              'POWER_FACTOR_SET', 'REACTIVE_POWER_SET', 'REACTIVE_POWER_PCT',
                              'ACTIVE_POWER_PCT', 'OPERATION_MODE', 'IV_SCAN_COMMAND', 'IV_SCAN_STATUS')
            for attr in sorted(dir(rm)):
                if attr.startswith('_'):
                    continue
                val = getattr(rm, attr)
                if isinstance(val, int) and 0 <= val <= 0xFFFF:
                    is_alias = any(attr == p or  # 정확히 일치하는 경우만 별칭
                                   (attr.startswith(p) and not attr[len(p):].startswith('_'))
                                   for p in alias_prefixes)
                    if val in addr_map and not is_alias:
                        addr_map[val] = attr
                    elif val not in addr_map:
                        addr_map[val] = attr
            patterns[proto] = addr_map
        except Exception:
            continue
    return patterns


def match_synonym(definition: str, synonym_db: dict) -> Optional[dict]:
    """
    제조사 레지스터 이름을 synonym_db와 매칭
    Returns: {'field': standard_name, 'category': cat, 'h01_field': h01_field} or None
    """
    if not definition:
        return None
    defn_lower = definition.strip().lower()
    defn_upper = to_upper_snake(definition)

    for field_name, info in synonym_db.get('fields', {}).items():
        # 정확히 일치
        if defn_upper == field_name:
            return {'field': field_name, 'category': info.get('category', ''),
                    'h01_field': info.get('h01_field', '')}
        # 동의어 매칭
        for syn in info.get('synonyms', []):
            if defn_lower == syn.lower() or defn_upper == to_upper_snake(syn):
                return {'field': field_name, 'category': info.get('category', ''),
                        'h01_field': info.get('h01_field', '')}
    return None


def match_synonym_fuzzy(definition: str, synonym_db: dict, threshold: float = 0.7) -> Optional[dict]:
    """
    퍼지 매칭 — 단어 겹침 기반
    """
    if not definition:
        return None
    words = set(re.findall(r'[a-zA-Z]+', definition.lower()))
    if not words:
        return None

    best_score = 0
    best_match = None
    for field_name, info in synonym_db.get('fields', {}).items():
        for syn in info.get('synonyms', []) + [field_name]:
            syn_words = set(re.findall(r'[a-zA-Z]+', syn.lower()))
            if not syn_words:
                continue
            overlap = len(words & syn_words)
            total = max(len(words), len(syn_words))
            score = overlap / total if total > 0 else 0
            if score > best_score:
                best_score = score
                best_match = {'field': field_name, 'category': info.get('category', ''),
                              'h01_field': info.get('h01_field', ''), 'score': score}
    if best_match and best_score >= threshold:
        return best_match
    return None


# ─── MPPT/String 패턴 추출 ───────────────────────────────────────────────────

MPPT_VOLTAGE_RE = re.compile(r'MPPT[_\s]*(\d+)[_\s]*(?:INPUT[_\s]*)?VOLTAGE', re.I)
MPPT_CURRENT_RE = re.compile(r'MPPT[_\s]*(\d+)[_\s]*(?:INPUT[_\s]*)?CURRENT', re.I)
MPPT_POWER_RE   = re.compile(r'MPPT[_\s]*(\d+)[_\s]*(?:INPUT[_\s]*)?POWER', re.I)
STRING_VOLTAGE_RE = re.compile(r'STRING[_\s]*(\d+)[_\s]*(?:INPUT[_\s]*)?VOLTAGE', re.I)
STRING_CURRENT_RE = re.compile(r'STRING[_\s]*(\d+)[_\s]*(?:INPUT[_\s]*)?CURRENT', re.I)
PV_VOLTAGE_RE = re.compile(r'PV[_\s]*(\d+)[_\s]*VOLTAGE', re.I)
PV_CURRENT_RE = re.compile(r'PV[_\s]*(\d+)[_\s]*CURRENT', re.I)


def detect_channel_number(definition: str) -> Optional[tuple]:
    """
    레지스터 이름에서 채널 번호 추출
    Returns: ('MPPT', n) or ('STRING', n) or None
    """
    for pat, prefix in [(MPPT_VOLTAGE_RE, 'MPPT'), (MPPT_CURRENT_RE, 'MPPT'),
                         (MPPT_POWER_RE, 'MPPT'), (PV_VOLTAGE_RE, 'MPPT'),
                         (PV_CURRENT_RE, 'MPPT'),
                         (STRING_VOLTAGE_RE, 'STRING'), (STRING_CURRENT_RE, 'STRING')]:
        m = pat.search(definition)
        if m:
            return (prefix, int(m.group(1)))
    return None


# ─── 주소 기반 레퍼런스 조회 ──────────────────────────────────────────────────

def get_ref_name_by_addr(addr: int, ref_patterns: Dict[str, Dict[int, str]]) -> Optional[str]:
    """주소로 레퍼런스 속성명 조회 (모든 프로토콜에서 탐색)"""
    if addr is None:
        return None
    for proto, addr_map in ref_patterns.items():
        if addr in addr_map:
            return addr_map[addr]
    return None


def get_h01_field_from_ref(addr: int, ref_patterns: Dict[str, Dict[int, str]],
                           synonym_db: dict) -> str:
    """주소 → 레퍼런스 속성명 → synonym_db → h01_field"""
    ref_name = get_ref_name_by_addr(addr, ref_patterns)
    if not ref_name:
        return ''
    # 레퍼런스 속성명으로 synonym_db 매칭
    result = match_synonym(ref_name, synonym_db)
    if result and result.get('h01_field'):
        return result['h01_field']
    # 채널 패턴에서 h01_field 추론
    ch = detect_channel_number(ref_name)
    if ch:
        prefix, n = ch
        ref_lower = ref_name.lower()
        if prefix == 'MPPT':
            if 'voltage' in ref_lower:
                return f'mppt{n}_voltage'
            if 'current' in ref_lower:
                return f'mppt{n}_current'
            if 'power' in ref_lower:
                return f'mppt{n}_power'
        elif prefix == 'STRING':
            if 'voltage' in ref_lower:
                return f'string{n}_voltage'
            if 'current' in ref_lower:
                return f'string{n}_current'
    return ''


def detect_channel_from_ref(addr: int, ref_patterns: Dict[str, Dict[int, str]]) -> Optional[tuple]:
    """주소로 레퍼런스 속성명을 찾아 채널 번호 추출"""
    ref_name = get_ref_name_by_addr(addr, ref_patterns)
    if ref_name:
        return detect_channel_number(ref_name)
    return None


# ─── Excel 유틸 ──────────────────────────────────────────────────────────────

def get_openpyxl():
    """openpyxl import with helpful error"""
    try:
        import openpyxl
        return openpyxl
    except ImportError:
        raise ImportError("openpyxl required: pip install openpyxl")


# ─── 카테고리 색상 ───────────────────────────────────────────────────────────

CATEGORY_COLORS = {
    'INFO':        'B4C6E7',
    'MONITORING':  'C6EFCE',
    'STATUS':      'FFE699',
    'ALARM':       'FFC7CE',
    'DER_CONTROL': 'D9D2E9',
    'DER_MONITOR': 'D5A6BD',
    'IV_SCAN':     'B7E1CD',
    'REVIEW':      'F4CCCC',
}

MATCH_COLORS = {
    'O': 'C6EFCE',   # 매칭 성공 - 초록
    'X': 'FFC7CE',   # 매칭 실패 - 빨강
    '-': 'D9D9D9',   # 해당 없음 - 회색
}
