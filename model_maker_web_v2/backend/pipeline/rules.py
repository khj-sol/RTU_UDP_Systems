# -*- coding: utf-8 -*-
"""
STAGE1_RULES.md + SKILL.md 규칙을 Python 로직으로 구현

규칙 원본: model_maker/STAGE1_RULES.md
각 함수에 해당 규칙 섹션 번호를 주석으로 표기
"""
import re
from typing import List, Dict, Optional, Set

from . import (
    RegisterRow, to_upper_snake, parse_address, detect_channel_number,
    match_synonym, match_synonym_fuzzy,
    get_ref_name_by_addr, get_h01_field_from_ref, detect_channel_from_ref,
)


# ═══════════════════════════════════════════════════════════════════════════
# §1. 추출 대상 판단
# ═══════════════════════════════════════════════════════════════════════════

def is_read_register(reg: RegisterRow) -> bool:
    """§1: 읽기(Read) 레지스터 위주, Write는 DER-AVM만 포함"""
    rw = (reg.rw or '').upper()
    if rw in ('WO', 'WRITE'):
        # DER-AVM 관련이면 포함
        defn = reg.definition.lower()
        if any(k in defn for k in ['power factor', 'reactive power', 'active power',
                                    'on/off', 'on off', 'operation mode', 'action mode',
                                    'curtailment', 'derate', 'iv scan', 'iv curve',
                                    'inverter control', 'power derating',
                                    '역률', '무효전력', '유효전력', '출력제한']):
            return True
        return False
    return True  # RO, RW, R/W 등


# ═══════════════════════════════════════════════════════════════════════════
# §2-1. H01 필수 추출 항목
# ═══════════════════════════════════════════════════════════════════════════

H01_REQUIRED_FIELDS = {
    'ac_voltage':     {'keywords': ['voltage', 'grid voltage', '전압', '계통전압'],
                       'unit': 'V', 'category': 'MONITORING'},
    'ac_current':     {'keywords': ['current', 'grid current', '전류', '계통전류'],
                       'unit': 'A', 'category': 'MONITORING'},
    'ac_power':       {'keywords': ['active power', 'output power', 'grid power',
                                    '유효전력', '출력전력', 'ac power'],
                       'unit': 'W', 'category': 'MONITORING'},
    'frequency':      {'keywords': ['frequency', '주파수', '계통주파수'],
                       'unit': 'Hz', 'category': 'MONITORING'},
    'power_factor':   {'keywords': ['power factor', 'pf', '역률', 'cos phi'],
                       'category': 'MONITORING'},
    'reactive_power': {'keywords': ['reactive power', 'q power', '무효전력'],
                       'unit': 'VAr', 'category': 'MONITORING'},
    'apparent_power': {'keywords': ['apparent power', 'va power', '피상전력'],
                       'unit': 'VA', 'category': 'MONITORING'},
    'daily_energy':   {'keywords': ['daily energy', 'today energy', '일일발전량',
                                    '금일발전량', 'daily yield', 'daily generation'],
                       'unit': 'kWh', 'category': 'MONITORING'},
    'total_energy':   {'keywords': ['total energy', 'cumulative energy', '누적발전량',
                                    '총발전량', 'lifetime energy', 'accumulated energy'],
                       'unit': 'kWh', 'category': 'MONITORING'},
    'inverter_status': {'keywords': ['inverter mode', 'running status', 'operating state',
                                     'inverter state', 'work mode', '인버터상태', '운전상태'],
                        'category': 'STATUS'},
    'temperature':    {'keywords': ['temperature', 'inner temp', 'heatsink temp',
                                    '온도', '방열판온도', '내부온도', 'module temp'],
                       'unit': '°C', 'category': 'MONITORING'},
    'alarm':          {'keywords': ['error code', 'fault code', 'alarm code', 'warning code',
                                    '오류코드', '고장코드', '알람', 'protection flag'],
                       'category': 'ALARM'},
}


# ═══════════════════════════════════════════════════════════════════════════
# §2-2. 알람 배분 규칙 (H01 alarm1/2/3)
# ═══════════════════════════════════════════════════════════════════════════

ALARM_PRIORITY = [
    # 1순위: 계통 보호
    {'keywords': ['grid over', 'grid under', 'over voltage', 'under voltage',
                  'over frequency', 'under frequency', 'over current',
                  '계통과전압', '계통저전압', '계통과주파수'],
     'priority': 1},
    # 2순위: 인버터 하드웨어 보호
    {'keywords': ['igbt', 'over temperature', 'short circuit', '과온', '단락', 'boost'],
     'priority': 2},
    # 3순위: PV 입력 보호
    {'keywords': ['pv over', 'pv under', 'dc over', 'pv insulation', 'string abnormal'],
     'priority': 3},
    # 4순위: 통신/기타
    {'keywords': ['communication', 'comm', 'eeprom', 'internal', 'logger', 'meter'],
     'priority': 4},
]


def _alarm_score(reg: RegisterRow) -> int:
    """알람 레지스터 우선순위 — 상태정의(Appendix 비트필드)가 있는 레지스터 우선
    PDF 정의 순서대로 alarm1/2/3 매칭. 같은 score면 주소순.
    """
    defn_lower = reg.definition.lower()
    addr = reg.address if isinstance(reg.address, int) else 0

    # ── 제외 (score 99) ──
    if addr > 0xFFFF:
        return 99
    # Appendix Fault Code 값이 레지스터로 잘못 추출 — 주소가 비정상적으로 작음
    # Goodwe는 주소 500+ 유효, EKOS SPD는 0x000B=11
    # Sungrow Appendix: 주소 0~600 범위의 fault code 값
    if 0 < addr < 2000 and not any(k in defn_lower for k in [
            'error_code', 'error code', 'error message',
            'fault code', 'fault/alarm code', 'alarm code',
            'fault status', 'hw fault', 'sw fault',
            'dsp alarm', 'dsp error', 'arm alarm',
            'warning code']):
        return 99
    # Work State 값 테이블 항목 (레지스터가 아닌 상태 값)
    if any(k in defn_lower for k in ['communicate fault', 'alarm run', 'derating run',
                                      'dispatch run', 'initial standby', 'key stop',
                                      'emergency stop']) or \
       (defn_lower.strip() in ('fault', 'stop', 'run', 'standby')):
        return 99
    # Fault/Alarm time — 시간 정보, 알람 레지스터 아님
    if any(k in defn_lower for k in ['fault/alarm time', 'alarm time', 'fault time']):
        return 99

    # ── 우선순위 ──
    # 0: 명확한 에러/폴트 코드 (정의 테이블 있음)
    if any(k in defn_lower for k in ['error_code', 'error code', 'fault code',
                                      'fault/alarm code', 'alarm code',
                                      'sw fault', 'dsp error', 'dsp alarm',
                                      'error message']):
        return 0
    # 1: HW Fault / PID / 번호 붙은 Alarm (Alarm 1, Alarm 2 — Huawei)
    if any(k in defn_lower for k in ['hw fault', 'hardware fault']):
        return 1
    if 'pid' in defn_lower and 'alarm' in defn_lower:
        return 1
    import re as _re
    if _re.search(r'\balarm\s*\d', defn_lower):
        return 1
    # 2: Grid Status / ARM alarm
    if any(k in defn_lower for k in ['grid status', 'grid fault', 'arm alarm', 'arm error']):
        return 2
    # 3: 일반 알람/폴트/워닝
    if any(k in defn_lower for k in ['alarm', 'fault', 'error', 'warning']):
        return 3
    # 9: 기타
    return 9


def distribute_alarms(alarm_regs: List[RegisterRow]) -> Dict[str, List[RegisterRow]]:
    """
    §2-2: 알람 레지스터를 H01 alarm1/2/3에 배분
    V2: score 99(제외) 레지스터는 alarm 슬롯에 넣지 않음 → N/A
    """
    # 우선순위 점수로 정렬 (같으면 주소순)
    scored = sorted(alarm_regs,
                    key=lambda r: (_alarm_score(r),
                                   r.address if isinstance(r.address, int) else 0))
    # score 99 제외 (Appendix 코드값, Work State 값 등)
    valid = [r for r in scored if _alarm_score(r) < 99]
    result = {'alarm1': [], 'alarm2': [], 'alarm3': [], 'dropped': []}

    slots = ['alarm1', 'alarm2', 'alarm3']
    for i, reg in enumerate(valid):
        if i < 3:
            result[slots[i]].append(reg)
        else:
            result['dropped'].append(reg)

    return result


# ═══════════════════════════════════════════════════════════════════════════
# §2-3. PV 입력 (MPPT/String) 규칙
# ═══════════════════════════════════════════════════════════════════════════

def get_pv_voltage_rule() -> str:
    """§2-3: PV 전압 계산 규칙"""
    return "pv_voltage = average(MPPTn_voltage for n if MPPTn_voltage > 100V)"


def get_pv_current_rule() -> str:
    """§2-3: PV 전류 계산 규칙 (String 우선)"""
    return ("1순위: sum(STRINGn_current for all n) [String 있는 경우]\n"
            "2순위: sum(MPPTn_current for all n) [String 없는 경우]")


def get_string_voltage_rule() -> str:
    """§2-3: String 전압 없으면 해당 MPPT 전압 사용"""
    return "STRING{n}_voltage = MPPT{ceil(n/strings_per_mppt)}_voltage (String 전압 레지스터 없는 경우)"


# ═══════════════════════════════════════════════════════════════════════════
# §3. 인버터 정보 필수 추출 항목
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# §3. INFO 블록 감지 — 검증된 이름 DB + 앵커(Model/SN) + RO
# ═══════════════════════════════════════════════════════════════════════════
#
# 제너럴 룰:
#   1. INFO 레지스터는 PDF 앞쪽에 위치 (Device information 섹션)
#   2. 모두 RO (Read-Only)
#   3. Model + SN이 앵커 — 하나라도 없으면 표시하여 RTU가 인지
#   4. 앵커 주변 연속 RO 레지스터 블록이 INFO
#   5. 검증된 인버터들의 INFO 이름 DB로 유사 이름 매칭

# ── 검증된 INFO 레지스터 이름 DB (5개 인버터에서 축적) ──
# 그룹별 정규화된 이름 패턴 — 새 인버터에서 유사 이름 자동 매칭
INFO_KNOWN_NAMES = {
    'MODEL': [
        'model', 'device model', 'device model name', 'device type',
        'device type code', 'type code', 'model id', 'model code',
        'model name', 'machine model', 'inverter model',
        'present identified model', 'lcd model setting',
        '모델', '모델명', '기기모델', '장비모델', '인버터모델', '제품모델',
    ],
    'SERIAL_NUMBER': [
        'sn', 'serial number', 'device serial number', 'device sn',
        'inverter sn', 'serialnumber',
        '시리얼', '시리얼번호', '제품번호', '일련번호', '기기번호', '장비번호',
    ],
    'PRODUCT_CODE': [
        'pn', 'product code', 'product number',
        '제품코드', '품목코드',
    ],
    'MAC_ADDRESS': [
        'plc mac address', 'mac address',
    ],
    'FIRMWARE': [
        'firmware version', 'firmware', 'master firmware version',
        'slave firmware version', 'software version', 'protocol version',
        'ems firmware version', 'lcd firmware version',
        'firmware version of arm', 'dsp version', 'arm version',
        'dsp1 test version', 'arm1 test version', 'dsp2 version',
        'dsp2 test version', 'communication version', 'hmi version',
        '펌웨어', '인버터 버전', '펌웨어 버전', '소프트웨어 버전',
        '마스터 펌웨어', '슬레이브 펌웨어', '통신 버전', '프로토콜 버전',
    ],
    'MPPT_COUNT': [
        'mppt number', 'number of mppts', 'mppt count', 'number of mppt',
        'mppt tracker', 'mppt수', 'mppt 수', 'mppt 개수',
    ],
    'PV_STRING_COUNT': [
        'number of pv strings', 'pv string count', 'string count',
        '스트링 수', '스트링수', 'pv 스트링',
    ],
    'RATED_POWER': [
        'nominal active power', 'rated power', 'nominal power',
        'maximum active power', 'max active power',
        'maximum apparent power', 'max apparent power',
        'rated active power',
        '정격출력', '인버터 용량', '정격용량', '정격전력', '최대출력',
        '공칭출력', '공칭전력', '피상전력정격',
    ],
    'RATED_VOLTAGE': [
        'nominal voltage', 'rated voltage',
        '정격전압', '공칭전압',
    ],
    'RATED_FREQUENCY': [
        'nominal frequency', 'rated frequency',
        '정격주파수', '공칭주파수',
    ],
    'PHASE': [
        'grid phase number', 'phase number', 'output type', 'phase count',
        'phase type',
        '상수', '위상수', '출력타입', '출력유형',
    ],
}

# 정규화된 이름 → 매칭용 (긴 키워드: 서브스트링, 짧은 키워드: 단어경계)
_INFO_KNOWN_LONG = set()   # len >= 6: 서브스트링 매칭
_INFO_KNOWN_SHORT = set()  # len < 6: 완전일치 또는 단어경계 매칭
_INFO_KNOWN_ALL = set()    # 전체 (완전일치용)
for _names in INFO_KNOWN_NAMES.values():
    for _n in _names:
        norm = _n.lower().replace(' ', '').replace('_', '')
        _INFO_KNOWN_ALL.add(norm)
        if len(norm) >= 6:
            _INFO_KNOWN_LONG.add(norm)
        else:
            _INFO_KNOWN_SHORT.add(norm)

# 짧은 키워드용 regex (단어 경계 매칭)
_INFO_SHORT_RE = re.compile(
    r'\b(' + '|'.join(re.escape(k) for k in _INFO_KNOWN_SHORT if len(k) >= 2) + r')\b',
    re.I
) if _INFO_KNOWN_SHORT else None


def is_known_info_name(definition: str) -> bool:
    """검증된 INFO 이름 DB에서 유사 이름 매칭.
    - 완전 일치 (정규화)
    - 긴 키워드 (≥6자): 서브스트링 매칭 — false positive 방지
    - 짧은 키워드 (<6자): 단어경계 regex 매칭 — 'model'이 'ThermalModel'에 안 걸리도록
    """
    defn_norm = definition.lower().replace(' ', '').replace('_', '').replace('-', '')
    # 1) 완전 일치
    if defn_norm in _INFO_KNOWN_ALL:
        return True
    # 2) 긴 키워드: 서브스트링 매칭 (≥6자만 — 'firmware', 'serialnumber' 등)
    for known in _INFO_KNOWN_LONG:
        if known in defn_norm:
            return True
    # 3) 짧은 키워드: 단어 경계 매칭 ('model', 'sn', 'pn' 등)
    if _INFO_SHORT_RE:
        defn_spaced = definition.replace('_', ' ').replace('-', ' ')
        if _INFO_SHORT_RE.search(defn_spaced):
            return True
    return False
#
# 앵커 키워드 (단어 경계 매칭, 제외 필터 적용):
_MODEL_RE = re.compile(
    r'\b(model|device\s*type|type\s*code|모델|모델명)\b', re.I)
_MODEL_EXCLUDE_RE = re.compile(
    r'meter|replace|target|sub.?device|third|label|lcd|inverter model definition', re.I)

_SN_RE = re.compile(
    r'\bsn\b|\bserial\b|serialnumber|시리얼|제품번호|시리얼번호', re.I)
_SN_EXCLUDE_RE = re.compile(
    r'alarm|clearance|license|board|layout|feature|monitor|third|label|historical|latest', re.I)

# INFO 블록 확장: is_known_info_name() (검증된 이름 DB) 사용

# INFO 블록 확장 시 중단하는 키워드 (운영 데이터 감지 → 블록 종료)
# INFO 블록 STOP — 일반 카테고리 기반 (특정 인버터 과적합 방지)
# 일반 카테고리 기반 STOP (특정 인버터 과적합 방지)
_INFO_BLOCK_STOP = re.compile(
    # 에너지 (발전량/소비량)
    r'energy|발전량|소비량|전력량|daily|monthly|yearly|total\s*power|'
    # 온도/환경
    r'temperature|temp\b|온도|'
    # 상태/알람/오류
    r'alarm|fault|error|warning|status|동작상태|발전유무|'
    # 측정값 (운영 데이터)
    r'insulation|절연|bus\s*volt|reactive\s*power|무효전력|real.?time|'
    # 시간 레지스터
    r'현재시각|시간|system\s*clock|time\s*stamp|sys\s*(year|month|day|hour|min|sec|weekly)|'
    # 운영 파라미터
    r'meter\b|sales|upgrade|subpackage|unique\s*id|country|'
    # 제어/설정값 (Growatt Vac/Fac, reset, flash, communication address)
    r'running\s*time|가동시간|mode\b|'
    r'\bvac\b|\bfac\b|reset|flash\s*start|com\s*address|fail\s*safe|'
    # 측정 전력 (PV input power, charge/discharge)
    r'\bppv\d|\bpcharge\b|\bpdischarge\b|\bpinv\b|\bprec\b|slave\s*is\s*busy|'
    # 목차/문서 구조 (Schneider 등)
    r'introduction|key\s*points|connection|configuration|logical\s*layer|'
    r'physical\s*layer|termination|communication\s*param',
    re.I
)

# 측정값(voltage/frequency/current): nominal/rated/grid 수식어 있으면 INFO OK
_INFO_STOP_MEASUREMENT = re.compile(
    r'(?<!nominal\s)(?<!rated\s)(?<!grid\s)voltage\b|'
    r'(?<!nominal\s)(?<!rated\s)(?<!grid\s)frequency\b|'
    r'(?<!nominal\s)(?<!rated\s)current\b',
    re.I
)

# INFO 블록 내 기본 최대 주소 간격 (동적 GAP으로 대체 — detect_info_block 내부)
_INFO_DEFAULT_GAP = 30


# PDF 섹션 제목 패턴 — "Device information" 등 (Device attributes는 제외 — 운영 레지스터 혼재)
_INFO_SECTION_RE = re.compile(
    r'device\s+(?:information|info)\b|'
    r'basic\s+(?:information|parameters)\b|'
    r'inverter\s+(?:\w+\s+)?information\b|equipment\s+information\b|'
    r'system\s+information\b|'
    r'장치\s*정보|기기\s*정보|설비\s*정보',
    re.I
)


def _find_info_section_pages(pages: list) -> list:
    """PDF 페이지에서 Device Information 섹션이 있는 페이지 번호 반환."""
    if not pages:
        return []
    result = []
    for p in pages:  # 전체 페이지 탐색 (섹션 제목은 어디든 있을 수 있음)
        if _INFO_SECTION_RE.search(p.get('text', '')):
            result.append(p['page'])
    return result


def detect_info_block(registers: list, pages: list = None) -> dict:
    """INFO 레지스터 블록 감지.

    우선순위:
    1. PDF 섹션 제목 ("Device information" 등) → 해당 페이지 테이블의 RO 레지스터
    2. Model/SN 앵커 기반 블록 확장 (섹션 제목 없을 때 fallback)

    Returns: {
        'info_addrs': set of addresses classified as INFO,
        'model_found': bool,
        'sn_found': bool,
        'model_addr': int or None,
        'sn_addr': int or None,
    }
    """
    from . import parse_address
    from .stage1 import extract_registers_from_tables, _detect_table_columns

    # RO 레지스터만 주소순 정렬
    ro_regs = []
    for reg in registers:
        rw = (getattr(reg, 'rw', '') or '').upper()
        if rw and rw not in ('RO', 'R', 'READ'):
            continue
        addr = reg.address if isinstance(reg.address, int) else parse_address(getattr(reg, 'address_hex', ''))
        if addr is None:
            continue
        ro_regs.append((addr, reg))
    ro_regs.sort(key=lambda x: x[0])

    if not ro_regs:
        return {'info_addrs': set(), 'model_found': False, 'sn_found': False,
                'model_addr': None, 'sn_addr': None}

    # ── 0단계: PDF 섹션 제목 기반 감지 (우선) ──
    if pages:
        info_pages = _find_info_section_pages(pages)
        if info_pages:
            # 해당 페이지의 첫 번째 테이블에서 레지스터 주소 수집
            section_addrs = set()
            for p in pages:
                if p['page'] in info_pages:
                    for tab in p.get('tables', []):
                        regs_from_tab = extract_registers_from_tables([tab])
                        for r in regs_from_tab:
                            rw = (getattr(r, 'rw', '') or '').upper()
                            if rw and rw not in ('RO', 'R', 'READ'):
                                continue
                            if r.address and isinstance(r.address, int):
                                section_addrs.add(r.address)
            if section_addrs:
                # 섹션 내에서도 앵커 + STOP 규칙 적용 (섹션에 운영 레지스터도 섞여 있을 수 있음)
                all_addrs = {a for a, _ in ro_regs}
                candidate_addrs = sorted(section_addrs & all_addrs)
                # 앵커 찾기
                model_addr = None
                sn_addr = None
                for addr, reg in ro_regs:
                    if addr not in candidate_addrs:
                        continue
                    defn = reg.definition.replace('_', ' ')
                    if not model_addr and _MODEL_RE.search(defn) and not _MODEL_EXCLUDE_RE.search(defn):
                        model_addr = addr
                    if not sn_addr and _SN_RE.search(defn) and not _SN_EXCLUDE_RE.search(defn):
                        sn_addr = addr
                # 앵커부터 STOP까지만 포함
                anchors_found = [a for a in [model_addr, sn_addr] if a]
                if anchors_found:
                    block_start = min(anchors_found)
                    # 동적 GAP
                    if len(anchors_found) >= 2:
                        max_gap = max(int(abs(anchors_found[0] - anchors_found[1]) * 1.5), _INFO_DEFAULT_GAP)
                    else:
                        max_gap = _INFO_DEFAULT_GAP
                    info_addrs = set()
                    prev = block_start
                    for addr in candidate_addrs:
                        if addr < block_start:
                            continue
                        reg_at = next((r for a, r in ro_regs if a == addr), None)
                        if not reg_at:
                            continue
                        defn = reg_at.definition.replace('_', ' ')
                        if addr in anchors_found:
                            info_addrs.add(addr)
                            prev = addr
                            continue
                        if _INFO_BLOCK_STOP.search(defn) or _INFO_STOP_MEASUREMENT.search(defn):
                            break
                        if is_known_info_name(defn) or (addr - prev <= 5):
                            info_addrs.add(addr)
                            prev = addr
                        elif addr - prev > max_gap:
                            break
                    return {
                        'info_addrs': info_addrs,
                        'model_found': model_addr is not None,
                        'sn_found': sn_addr is not None,
                        'model_addr': model_addr,
                        'sn_addr': sn_addr,
                    }

    # ── 1단계: 앵커 탐색 (첫 번째 매칭만) — 섹션 제목 없을 때 fallback ──
    model_anchor = None  # (addr, reg)
    sn_anchor = None
    for addr, reg in ro_regs:
        defn = reg.definition.replace('_', ' ')
        if not model_anchor and _MODEL_RE.search(defn) and not _MODEL_EXCLUDE_RE.search(defn):
            model_anchor = (addr, reg)
        if not sn_anchor and _SN_RE.search(defn) and not _SN_EXCLUDE_RE.search(defn):
            sn_anchor = (addr, reg)
        if model_anchor and sn_anchor:
            break

    model_found = model_anchor is not None
    sn_found = sn_anchor is not None

    if not model_found and not sn_found:
        return {'info_addrs': set(), 'model_found': False, 'sn_found': False,
                'model_addr': None, 'sn_addr': None}

    anchors = [a for a in [model_anchor, sn_anchor] if a]
    block_start = min(a[0] for a in anchors)

    # 동적 GAP: 앵커 간 거리 기반 (Model↔SN 거리의 1.5배, 최소 30)
    if len(anchors) >= 2:
        anchor_dist = abs(anchors[0][0] - anchors[1][0])
        max_gap = max(int(anchor_dist * 1.5), _INFO_DEFAULT_GAP)
    else:
        max_gap = _INFO_DEFAULT_GAP

    # ── 2단계: 앵커부터 전방 확장 ──
    # block_start 이전 레지스터도 포함 (SN이 Model 앞에 올 수 있음)
    info_addrs = set()
    prev_addr = block_start

    for addr, reg in ro_regs:
        if addr < block_start:
            # 앵커 직전 레지스터도 포함 (앵커 간 간격 이내)
            if block_start - addr <= max_gap:
                defn = reg.definition.replace('_', ' ')
                if is_known_info_name(defn) and \
                        not _INFO_BLOCK_STOP.search(defn) and not _INFO_STOP_MEASUREMENT.search(defn):
                    info_addrs.add(addr)
            continue

        # 주소 간격 초과 → 블록 종료
        if addr - prev_addr > max_gap and addr not in {a[0] for a in anchors}:
            break

        defn = reg.definition.replace('_', ' ')

        # 앵커 주소는 무조건 포함
        if addr in {a[0] for a in anchors}:
            info_addrs.add(addr)
            prev_addr = addr
            continue

        # 운영 데이터 키워드 → 블록 종료
        if _INFO_BLOCK_STOP.search(defn) or _INFO_STOP_MEASUREMENT.search(defn):
            break

        # INFO 키워드 매칭 → 포함 (간격 MAX_GAP 이내)
        if is_known_info_name(defn):
            info_addrs.add(addr)
            prev_addr = addr
        elif addr - prev_addr <= 5:
            # 키워드 불일치지만 앵커 근접 → 포함
            info_addrs.add(addr)
            prev_addr = addr
        else:
            # 키워드 불일치 + 간격 큼 → 블록 종료
            break

    # ── 3단계: 산재 모드 (scattered) — 블록이 너무 작으면 전체 RO에서 개별 수집 ──
    # GoodWe 등 INFO가 흩어진 인버터: 블록 ≤ 2개이면 검증된 이름 DB로 전체 탐색
    if len(info_addrs) <= 2:
        _SCATTERED_EXCLUDE_RE = re.compile(
            r'alarm|clearance|license|board|layout|monitor|third|label|'
            r'meter|replace|target|sub.?device|historical|latest|'
            r'error|fault|warning',
            re.I
        )
        scattered_addrs = set()
        for addr, reg in ro_regs:
            defn = reg.definition.replace('_', ' ')
            if is_known_info_name(defn) and not _SCATTERED_EXCLUDE_RE.search(defn):
                scattered_addrs.add(addr)
        if len(scattered_addrs) > len(info_addrs):
            info_addrs = scattered_addrs
            # 앵커 재확인
            model_anchor = None
            sn_anchor = None
            for addr, reg in ro_regs:
                if addr not in info_addrs:
                    continue
                defn = reg.definition.replace('_', ' ')
                if not model_anchor and _MODEL_RE.search(defn) and not _MODEL_EXCLUDE_RE.search(defn):
                    model_anchor = (addr, reg)
                if not sn_anchor and _SN_RE.search(defn) and not _SN_EXCLUDE_RE.search(defn):
                    sn_anchor = (addr, reg)
            model_found = model_anchor is not None
            sn_found = sn_anchor is not None

    return {
        'info_addrs': info_addrs,
        'model_found': model_found,
        'sn_found': sn_found,
        'model_addr': model_anchor[0] if model_anchor else None,
        'sn_addr': sn_anchor[0] if sn_anchor else None,
    }


def is_info_register(definition: str) -> bool:
    """§3: 인버터 정보 레지스터 여부 — detect_info_block 사용 시 호출하지 않음.
    하위 호환용: 단독 호출 시 키워드 기반 판별."""
    defn_norm = definition.lower().replace(' ', '').replace('_', '')
    for k in ['model', 'serial', 'firmware', 'software', 'protocolversion',
              'mppt', 'ratedpower', 'nominalpower', 'maximumactive',
              'outputtype', 'devicetype', 'phasenumber']:
        if k in defn_norm:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════
# §4. DER-AVM 제어/모니터링 규칙
# ═══════════════════════════════════════════════════════════════════════════

DER_CONTROL_KEYWORDS = [
    'power limit', 'output regulation', 'curtailment', 'derate',
    'power factor set', 'pf set', 'cos phi set', 'pf control',
    'reactive power set', 'q set', 'var set', 'q control',
    'on/off', 'on off', 'start/stop', 'remote on',
    'operation mode', 'running mode set', 'control mode', 'power control mode',
    'active power limit', 'active power curtailment',
    '출력제한', '역률설정', '무효전력설정', '운전모드설정', '기동정지',
]


def is_der_control(reg: RegisterRow) -> bool:
    """§4: DER-AVM 제어 레지스터 여부 — Write 가능 + 키워드 매칭"""
    rw = (reg.rw or '').upper()
    if rw not in ('RW', 'WO', 'R/W'):
        return False
    defn_lower = reg.definition.lower()
    return any(k in defn_lower for k in DER_CONTROL_KEYWORDS)


# ═══════════════════════════════════════════════════════════════════════════
# §5. IV 스캔 지원 판단
# ═══════════════════════════════════════════════════════════════════════════

IV_SCAN_KEYWORDS = [
    'iv scan', 'iv curve', 'i-v scan', 'iv test',
    'iv start', 'start iv', 'iv trigger',
    'iv status', 'iv state', 'iv result',
    'iv point', 'iv data', 'string iv',
]

# 확정 IV 스캔 지원 제조사
IV_SCAN_CONFIRMED = {'solarize', 'kstar', 'senergy'}


def detect_iv_scan_support(registers: List[RegisterRow], manufacturer: str) -> bool:
    """§5: IV 스캔 지원 여부 판단"""
    # 확정 제조사
    if manufacturer.lower() in IV_SCAN_CONFIRMED:
        return True
    # 레지스터 키워드 탐색
    for reg in registers:
        defn_lower = reg.definition.lower()
        if any(k in defn_lower for k in IV_SCAN_KEYWORDS):
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════
# §6. 제외 항목
# ═══════════════════════════════════════════════════════════════════════════

EXCLUDE_KEYWORDS = [
    # 통신 설정
    'baud rate', 'baudrate', 'slave id', 'slave address', 'communication setting',
    'comm setting', 'rs485', 'modbus addr', 'protocol version', 'comm port',
    'wifi', 'ssid', 'password', 'digital meter modbus',
    # 보호 파라미터 설정 (§6)
    'over voltage protection', 'under voltage protection',
    'over frequency protection', 'under frequency protection',
    'over current protection', 'protection setting', 'protection value',
    'trip point', 'trip time', 'trip_time', 'reconnect time', 'anti-islanding',
    'loss level', 'loss_level',
    'triggering voltage', 'triggering threshold',
    'lvrt', 'hvrt', 'frt mode', 'frt_mode',
    'fault ride', 'ride through',
    'detection',  # insulation/ground detection settings
    # 날짜/시간 레지스터
    'dateyear', 'datemonth', 'datesecond',
    # 기타 설정
    'first connect', 'soft start', 'start time',
    # 'device type' — 제거 (INFO의 Device type code와 충돌)
    # REMS (§6)
    'rems', 'remote monitoring system',
    # V2: 제어 관련 레지스터 제외 (DER-AVM으로만 제어)
    'start/stop', 'remote start', 'remote stop', 'emergency stop',
    'power on command', 'shut down command',
    # V2: Q(P)/Q(U) 커브, 모델명 테이블
    'q(p)', 'q(u)', 'q(v)', 'qu curve', 'qp curve',
    'uin', 'uout', 'u1 limit', 'u2 limit', 'ulimit',
    'hysteresis', 'curve(italy)', 'v1i(', 'v2i(', 'v1s(', 'v2s',
    'qmax(', 'pin(italy)', 'pout(italy)',
    'enablemode', 'lower q/sn', 'upper q/sn',
    # V2: 시스템 클럭/예약
    'system clock', 'reserved',
    # V2: PID 관련 설정
    'anti-pid', 'pid suppression', 'pid impedance', 'pid function',
    # V2: 국가코드/지역코드 값
    'great britain', 'us-ne', 'us-sa', 'us-',
    # V2: 기타 설정값/커브
    'upper u limit', 'lower u limit',
    'key stop', 'dispatch run', 'derating run',
    # V2: Fault 코드 테이블 엔트리 (이름이 짧은 상태 설명)
    'stop bit', 'data bit', 'output overload',
    'pv input configuration',
    # V2: Fuse 관련 (H01/INFO 불필요)
    'fuse open', 'fuse check', 'fuse data',
]

# 보호 파라미터 주소 범위 (§6: 0x5000~ 보호설정 블록)
EXCLUDE_ADDR_RANGES = [
    (0x5000, 0x51FF),   # 보호 파라미터 설정 블록
    (0x3000, 0x3005),   # 날짜/시간 설정
    (0x3060, 0x307F),   # WiFi 설정
    (0x30B0, 0x30B1),   # Digital meter/device type 설정
]

# 이름 길이 제한
MAX_NAME_LENGTH = 60


def should_exclude(reg: RegisterRow) -> bool:
    """§6: 제외 항목 판단"""
    defn_lower = reg.definition.lower()
    defn_upper = to_upper_snake(reg.definition)

    # 키워드 제외
    for kw in EXCLUDE_KEYWORDS:
        if kw in defn_lower:
            return True

    # 주소 범위 제외
    addr = reg.address if isinstance(reg.address, int) else parse_address(str(reg.address))
    if addr is not None:
        for lo, hi in EXCLUDE_ADDR_RANGES:
            if lo <= addr <= hi:
                return True

    # 이름이 너무 길면 제외 (PDF에서 설명문이 이름으로 추출된 경우)
    if len(defn_upper) > MAX_NAME_LENGTH:
        return True

    return False


# ═══════════════════════════════════════════════════════════════════════════
# §7. 카테고리 분류 (REVIEW 기준 포함)
# ═══════════════════════════════════════════════════════════════════════════

REVIEW_REASONS = {
    'ambiguous_h01':      'H01 필드 대응이 애매 — 동일 물리량에 여러 레지스터',
    'unit_unusual':       '단위/스케일이 특이 — 변환 방법 불확실',
    'der_unclear':        'DER-AVM 해당 여부 불분명 — Write 레지스터',
    'alarm_undefined':    '알람 비트 정의 미확인',
    'duplicate_quantity': '같은 물리량에 여러 레지스터가 존재',
    'unclassifiable':     '자동 분류 불가 — 레퍼런스/동의어 매칭 없음',
}


def classify_register_with_rules(
    reg: RegisterRow,
    synonym_db: dict,
    review_history: dict,
    ref_patterns: dict,
    device_type: str = 'inverter',
    all_regs: List[RegisterRow] = None,
    info_addrs: set = None,
) -> tuple:
    """
    §7: STAGE1_RULES에 따른 카테고리 분류
    Returns: (category, review_reason)
    """
    defn = reg.definition
    defn_lower = defn.lower()
    defn_upper = to_upper_snake(defn)
    addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)

    # §6: 제외 항목
    if should_exclude(reg):
        return ('EXCLUDE', '')

    # 0) INFO 블록 — detect_info_block()으로 사전 감지된 주소 집합
    if info_addrs and addr is not None and addr in info_addrs:
        return ('INFO', '')

    # 1) 레퍼런스 패턴 기반 (주소 매칭)
    if addr is not None:
        for proto, addr_map in ref_patterns.items():
            if addr in addr_map:
                ref_name = addr_map[addr].upper()
                if 'DEA_' in ref_name:
                    return ('DER_MONITOR' if device_type == 'inverter' else 'EXCLUDE', '')
                if any(k in ref_name for k in ['DER_', 'INVERTER_ON_OFF', 'INVERTER_CONTROL',
                                                'POWER_FACTOR_DYNAMIC', 'REACTIVE_POWER_DYNAMIC',
                                                'POWER_DERATING', 'CLEAR_PV', 'EEPROM']):
                    return ('DER_CONTROL' if device_type == 'inverter' else 'EXCLUDE', '')
                if 'IV_' in ref_name:
                    return ('IV_SCAN' if device_type == 'inverter' else 'EXCLUDE', '')
                if any(k in ref_name for k in ['ERROR_CODE', 'ALARM', 'FAULT', 'WARNING']):
                    return ('ALARM', '')
                if any(k in ref_name for k in ['INVERTER_MODE', 'RUNNING_STATUS', 'STATUS']):
                    return ('STATUS', '')
                # INFO는 키워드(is_info_register)에서만 분류
                # reference pattern 주소 기반 INFO 분류 제거 — 제조사 간 주소 충돌 방지
                return ('MONITORING', '')

    # 2) INFO는 detect_info_block()의 info_addrs로만 분류 (step 0에서 처리됨)

    # 3) synonym_db 매칭
    syn = match_synonym(defn, synonym_db)
    if syn:
        cat = syn['category']
        if cat in ('DER_CONTROL', 'DER_MONITOR', 'IV_SCAN') and device_type != 'inverter':
            return ('EXCLUDE', '')
        return (cat, '')

    # §2-2: STATUS 키워드 (단, Fault/Alarm/Grid Status는 ALARM 우선)
    if any(k in defn_lower for k in ['fault status', 'alarm status', 'fault state',
                                      'grid status']):
        return ('ALARM', '')
    if any(k in defn_lower for k in ['status', 'state', 'mode', 'running',
                                      '상태', '운전', '동작']):
        # §7: 동일 물리량 중복 체크
        if all_regs:
            similar = [r for r in all_regs if r is not reg and
                       any(k in r.definition.lower() for k in ['status', 'state', 'mode'])]
            if len(similar) > 2:
                return ('STATUS', '')  # 여러 개여도 STATUS
        return ('STATUS', '')

    # 5) §2-2: ALARM 키워드
    if any(k in defn_lower for k in ['alarm', 'fault', 'error', 'warning', 'protection flag',
                                      '알람', '고장', '경보', '오류']):
        return ('ALARM', '')

    # 6) §5: IV Scan 키워드 (인버터만)
    if device_type == 'inverter':
        if any(k in defn_lower for k in IV_SCAN_KEYWORDS):
            return ('IV_SCAN', '')

    # 7) §4: DER 키워드 (인버터만)
    if device_type == 'inverter' and is_der_control(reg):
        return ('DER_CONTROL', '')

    # 8) MPPT/String → MONITORING
    ch = detect_channel_number(defn)
    if ch:
        return ('MONITORING', '')

    # 9) §2-1: H01 필수 항목 키워드
    for field_name, field_info in H01_REQUIRED_FIELDS.items():
        if any(k in defn_lower for k in field_info['keywords']):
            return (field_info['category'], '')

    # 10) 일반 모니터링 키워드
    if any(k in defn_lower for k in ['voltage', 'current', 'power', 'energy', 'frequency',
                                      'temperature', 'factor', '전압', '전류', '전력',
                                      '발전량', '온도', '주파수', '역률']):
        return ('MONITORING', '')

    # 11) 퍼지 매칭
    fuzzy = match_synonym_fuzzy(defn, synonym_db, threshold=0.6)
    if fuzzy:
        cat = fuzzy['category']
        if cat in ('DER_CONTROL', 'DER_MONITOR', 'IV_SCAN') and device_type != 'inverter':
            return ('EXCLUDE', '')
        return (cat, '')

    # 12) review_history 동일 패턴
    for item in review_history.get('approved', []):
        if item.get('definition', '').upper() == defn_upper:
            verdict = item.get('verdict', '')
            if verdict == 'DELETE':
                return ('EXCLUDE', '')
            if verdict.startswith('MOVE:'):
                return (verdict.replace('MOVE:', ''), '')

    # V2: 분류 불가 → 자동 분류 (REVIEW 최소화)
    # REVIEW는 H01/IV 매칭에서 진짜 헷갈리는 경우만

    # "abnormal" → ALARM
    if 'abnormal' in defn_lower:
        return ('ALARM', '')

    # Write 레지스터인데 제어도 모니터링도 아닌 경우 → 제외
    if reg.rw in ('RW', 'WO', 'R/W') and device_type == 'inverter':
        return ('EXCLUDE', '')

    # 설명문/디버깅/내부 사용 → 제외
    if any(k in defn_lower for k in ['debugging', 'internal use', 'register for',
                                      'tens place', 'read keep', 'read input',
                                      'single write', 'rotational speed',
                                      'coefficient']):
        return ('EXCLUDE', '')

    # 나머지 → MONITORING으로 자동 포함
    return ('MONITORING', '')


# ═══════════════════════════════════════════════════════════════════════════
# §8. Stage 1/2 역할 분리
# ═══════════════════════════════════════════════════════════════════════════

def filter_channels_stage2(
    registers: List[RegisterRow],
    mppt_count: int,
    total_strings: int,
    ref_patterns: dict = None,
) -> List[RegisterRow]:
    """
    §8: Stage 2 필터링
    - MPPT{n} → n <= mppt_count
    - STRING{n} → n <= total_strings
    - IV Scan 트래커 → tracker <= mppt_count
    - INFO, MONITORING(비채널), STATUS, ALARM, DER → 전부 포함
    """
    filtered = []
    for reg in registers:
        # 이름에서 채널 감지
        ch = detect_channel_number(reg.definition)
        if not ch:
            # 주소에서 레퍼런스 채널 감지
            addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)
            if addr is not None and ref_patterns:
                ch = detect_channel_from_ref(addr, ref_patterns)
        if ch:
            prefix, n = ch
            if prefix == 'MPPT' and n > mppt_count:
                continue
            if prefix == 'STRING' and n > total_strings:
                continue
        filtered.append(reg)
    return filtered


# ═══════════════════════════════════════════════════════════════════════════
# §9. Stage 2 인계 원칙
# ═══════════════════════════════════════════════════════════════════════════

def should_include_ambiguous(reg: RegisterRow) -> bool:
    """§9: 애매한 항목은 제외하지 않고 포함"""
    return True  # 항상 포함, REVIEW로 분류하여 Stage 2에서 처리


# ═══════════════════════════════════════════════════════════════════════════
# 설비 타입별 카테고리 필터
# ═══════════════════════════════════════════════════════════════════════════

DEVICE_CATEGORIES = {
    'inverter': ['INFO', 'MONITORING', 'STATUS', 'ALARM',
                 'DER_CONTROL', 'DER_MONITOR', 'IV_SCAN', 'REVIEW'],
    'relay':    ['INFO', 'MONITORING', 'STATUS', 'ALARM', 'REVIEW'],
    'weather':  ['INFO', 'MONITORING', 'STATUS', 'ALARM', 'REVIEW'],
}


def get_valid_categories(device_type: str) -> List[str]:
    """설비 타입별 유효 카테고리"""
    return DEVICE_CATEGORIES.get(device_type, DEVICE_CATEGORIES['inverter'])


# ═══════════════════════════════════════════════════════════════════════════
# V2: H01 DER 겹침 필드 판단
# ═══════════════════════════════════════════════════════════════════════════

# H01에서 DER 주소를 사용하는 9개 필드의 키워드
# 이 필드들은 PDF에서 매핑 불필요 (DER 고정 주소맵으로 자동 삽입)
H01_DER_OVERLAP_KEYWORDS = {
    'r_voltage':    ['phase a voltage', 'l1 voltage', 'r-phase voltage', 'phase r voltage',
                     'ua', 'grid a voltage', 'a-n voltage', 'a phase voltage'],
    's_voltage':    ['phase b voltage', 'l2 voltage', 's-phase voltage', 'phase s voltage',
                     'ub', 'grid b voltage', 'b-n voltage', 'b phase voltage'],
    't_voltage':    ['phase c voltage', 'l3 voltage', 't-phase voltage', 'phase t voltage',
                     'uc', 'grid c voltage', 'c-n voltage', 'c phase voltage'],
    'r_current':    ['phase a current', 'l1 current', 'r-phase current', 'phase r current',
                     'ia', 'grid a current', 'a phase current'],
    's_current':    ['phase b current', 'l2 current', 's-phase current', 'phase s current',
                     'ib', 'grid b current', 'b phase current'],
    't_current':    ['phase c current', 'l3 current', 't-phase current', 'phase t current',
                     'ic', 'grid c current', 'c phase current'],
    'ac_power':     ['total active power', 'total output power', 'grid active power',
                     'active power', 'output power', 'total power output'],
    'power_factor': ['power factor', 'cos phi', 'pf'],
    'frequency':    ['grid frequency', 'output frequency', 'frequency'],
}


def is_h01_der_overlap(reg: RegisterRow) -> Optional[str]:
    """
    V2: H01 DER 겹침 필드인지 판단
    Returns: 겹치는 H01 필드명 or None
    """
    defn_lower = reg.definition.lower()
    for h01_field, keywords in H01_DER_OVERLAP_KEYWORDS.items():
        if any(k in defn_lower for k in keywords):
            return h01_field
    return None
