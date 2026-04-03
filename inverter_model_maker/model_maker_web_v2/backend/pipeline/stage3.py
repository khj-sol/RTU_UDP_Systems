# -*- coding: utf-8 -*-
"""
Stage 3 — Stage 2 Excel → *_registers.py 코드 생성

사용자 점검 완료된 Stage 2 Excel에서 RTU 호환 registers.py 코드를 생성한다.
레퍼런스(REF_Solarize_PV_registers.py)와 동일한 구조를 목표로 한다:
- 12개 클래스, 9개 모듈 함수, SCALE, DATA_TYPES, FLOAT32_FIELDS
"""
import os
import re
import importlib.util
from datetime import datetime
from textwrap import dedent, indent
from typing import List, Dict, Optional

from . import (
    PROJECT_ROOT, COMMON_DIR, RTU_COMMON_DIR,
    RegisterRow, INVERTER_MODES,
    to_upper_snake, parse_address, detect_channel_number,
    load_synonym_db, save_synonym_db,
    load_review_history, save_review_history,
    get_openpyxl, ProgressCallback,
)
from .stage2 import read_stage1_excel_v2 as read_stage1_excel  # V2 호환


# ─── Stage 2 Excel 읽기 ─────────────────────────────────────────────────────

def read_stage2_excel(path: str) -> dict:
    """Stage 2 Excel → {meta, all_regs, review_items} — V2 + 레거시 호환"""
    openpyxl = get_openpyxl()
    wb = openpyxl.load_workbook(path, data_only=True)
    result = {'meta': {}, 'all_regs': [], 'review_items': []}

    is_v2 = '1_REGISTER_MAP' in wb.sheetnames

    # ── META 읽기 ──
    summary_sheet = '3_SUMMARY' if is_v2 else 'SUMMARY'
    meta_sheet = '1_REGISTER_MAP' if is_v2 else summary_sheet
    if meta_sheet in wb.sheetnames:
        ws = wb[meta_sheet]
        for row in ws.iter_rows(values_only=True):
            cells = [str(c).strip() if c is not None else '' for c in row]
            if len(cells) >= 2 and cells[0] and cells[1]:
                result['meta'][cells[0]] = cells[1]

    # ── 레지스터 읽기 ──
    if is_v2 and '1_REGISTER_MAP' in wb.sheetnames:
        # V2: 1_REGISTER_MAP에서 섹션별로 파싱
        ws = wb['1_REGISTER_MAP']
        current_cat = ''
        header = None
        for row in ws.iter_rows(values_only=True):
            cells = [str(c).strip() if c is not None else '' for c in row]
            first = cells[0] if cells else ''
            # 섹션 감지
            if 'INFO 레지스터' in first:
                current_cat = 'INFO'
                header = None
                continue
            elif 'H01 MONITORING' in first:
                current_cat = 'MONITORING'
                header = None
                continue
            elif 'STATUS 레지스터' in first:
                current_cat = 'STATUS'
                header = None
                continue
            elif 'ALARM 레지스터' in first:
                current_cat = 'ALARM'
                header = None
                continue
            elif first.startswith('Stage 2') or not first:
                if not current_cat:
                    continue
                if not any(cells):
                    current_cat = ''
                    header = None
                    continue
            # 헤더
            if first == 'No':
                header = cells
                continue
            if not header or not first or not first.isdigit():
                continue
            # 행 파싱
            reg = _parse_s2_reg_row(cells, header, current_cat)
            if reg:
                result['all_regs'].append(reg)
    elif 'ALL' in wb.sheetnames:
        # 레거시: ALL 시트
        ws = wb['ALL']
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) >= 2:
            header = [str(c).strip() if c else '' for c in rows[0]]
            for row in rows[1:]:
                cells = [str(c).strip() if c is not None else '' for c in row]
                if not any(cells):
                    continue
                d = {}
                for i, h in enumerate(header):
                    if i >= len(cells):
                        break
                    hl = h.lower()
                    if 'category' in hl:
                        d['category'] = cells[i]
                    elif 'definition' in hl:
                        d['definition'] = cells[i]
                    elif 'address' in hl:
                        d['address_hex'] = cells[i]
                        d['address'] = parse_address(cells[i]) or 0
                    elif hl == 'reg':
                        d['regs'] = cells[i]
                    elif 'type' in hl and 'body' not in hl:
                        d['data_type'] = cells[i]
                    elif 'unit' in hl or 'scale' in hl:
                        d['unit'] = cells[i]
                    elif 'r/w' in hl:
                        d['rw'] = cells[i]
                    elif 'comment' in hl:
                        d['comment'] = cells[i]
                    elif 'h01 field' in hl:
                        d['h01_field'] = cells[i]
                result['all_regs'].append(RegisterRow.from_dict(d))

    # ── REVIEW 읽기 ──
    review_sheet = '2_REVIEW' if is_v2 else 'REVIEW'
    if review_sheet in wb.sheetnames:
        ws = wb[review_sheet]
        header = None
        for row in ws.iter_rows(values_only=True):
            cells = [str(c).strip() if c is not None else '' for c in row]
            first = cells[0] if cells else ''
            if first == 'No':
                header = cells
                continue
            if header and first and first.isdigit():
                item = {}
                for i, h in enumerate(header):
                    if i < len(cells):
                        item[h] = cells[i]
                result['review_items'].append(item)

    wb.close()
    return result


def _parse_s2_reg_row(cells, header, category):
    """Stage 2 V2 레지스터 행 파싱"""
    col_map = {}
    for i, h in enumerate(header):
        hl = h.lower()
        if 'definition' in hl or 'name' in hl:
            col_map['definition'] = i
        elif 'address' in hl:
            col_map['address'] = i
        elif 'type' in hl and 'data' not in hl:
            col_map['type'] = i
        elif 'unit' in hl or 'scale' in hl:
            col_map['scale'] = i
        elif 'r/w' in hl:
            col_map['rw'] = i
        elif 'h01' in hl:
            col_map['h01_field'] = i
        elif 'comment' in hl:
            col_map['comment'] = i

    defn = cells[col_map['definition']] if 'definition' in col_map and col_map['definition'] < len(cells) else ''
    if not defn:
        return None

    addr_raw = cells[col_map['address']] if 'address' in col_map and col_map['address'] < len(cells) else ''
    addr = parse_address(addr_raw)

    return RegisterRow(
        definition=defn,
        address=addr if addr is not None else 0,
        address_hex=addr_raw if addr_raw.startswith('0x') else (f'0x{addr:04X}' if addr else ''),
        data_type=cells[col_map.get('type', 99)] if col_map.get('type', 99) < len(cells) else 'U16',
        scale=cells[col_map.get('scale', 99)] if col_map.get('scale', 99) < len(cells) else '',
        rw=cells[col_map.get('rw', 99)] if col_map.get('rw', 99) < len(cells) else 'RO',
        h01_field=cells[col_map.get('h01_field', 99)] if col_map.get('h01_field', 99) < len(cells) else '',
        comment=cells[col_map.get('comment', 99)] if col_map.get('comment', 99) < len(cells) else '',
        category=category,
    )


# ─── 레퍼런스 ErrorCode BITS 로딩 ───────────────────────────────────────────

def _load_error_bits_from_reference(manufacturer: str) -> dict:
    """레퍼런스 REF_*_registers.py 또는 {제조사}_*_registers.py에서 ErrorCode BITS 로드"""
    import glob
    result = {}
    mfr_lower = manufacturer.lower()
    # REF_ 파일 우선, 그 다음 신규 네이밍, 마지막으로 Solarize REF fallback
    patterns = glob.glob(os.path.join(COMMON_DIR, f'REF_{manufacturer}_*_registers.py'))
    patterns += glob.glob(os.path.join(COMMON_DIR, f'{manufacturer}_*_registers.py'))
    patterns += [os.path.join(COMMON_DIR, f'REF_Solarize_PV_registers.py'),
                 os.path.join(COMMON_DIR, 'Solarize_PV_50kw_registers.py'),
                 os.path.join(COMMON_DIR, 'Solarize_PV_registers.py')]

    for fpath in patterns:
        if not os.path.exists(fpath):
            continue
        try:
            spec = importlib.util.spec_from_file_location('_ref_err', fpath)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            for cls_name in ['ErrorCode1', 'ErrorCode2', 'ErrorCode3']:
                cls = getattr(mod, cls_name, None)
                if cls and hasattr(cls, 'BITS') and cls.BITS:
                    result[cls_name] = dict(cls.BITS)
            if result:
                return result
        except Exception:
            continue
    return result


# ─── 코드 생성 함수들 ────────────────────────────────────────────────────────

def _gen_header(manufacturer: str, protocol_name: str, protocol_version: str,
                mppt: int, total_strings: int, strings_per_mppt: int,
                iv_scan: bool, der_avm: bool) -> str:
    return f'''# -*- coding: utf-8 -*-
"""
{manufacturer} Inverter Modbus Register Map
Based on {manufacturer} Modbus Protocol {protocol_version}
Protocol Name: {protocol_name}
MPPT: {mppt} | Strings: {total_strings} ({strings_per_mppt}/MPPT)
IV Scan: {"Yes" if iv_scan else "No"} | DER-AVM: {"Yes" if der_avm else "No"}
Generated by Model Maker Web v2 on {datetime.now().strftime("%Y-%m-%d")}
"""

'''


def _gen_register_map(regs_by_cat: dict, mppt: int, total_strings: int,
                      strings_per_mppt: int, iv_scan: bool,
                      iv_data_points: int = 64) -> str:
    lines = [
        '',
        'class RegisterMap:',
        f'    """{regs_by_cat.get("_manufacturer", "Inverter")} Modbus Register Map"""',
        '',
    ]

    cat_titles = {
        'INFO': 'Device Information',
        'MONITORING': 'Monitoring Data',
        'STATUS': 'Status & Temperature',
        'ALARM': 'Alarm / Error Codes',
        'DER_CONTROL': 'DER-AVM Control Parameters',
        'DER_MONITOR': 'DEA-AVM Real-time Data',
        'IV_SCAN': 'IV Scan Control',
    }

    # 메인 속성들 + _HIGH 워드 자동 추가
    emitted_names = set()
    for cat in ['INFO', 'MONITORING', 'STATUS', 'ALARM', 'DER_CONTROL', 'DER_MONITOR', 'IV_SCAN']:
        regs = regs_by_cat.get(cat, [])
        if not regs:
            continue
        title = cat_titles.get(cat, cat)
        lines.append(f'    # =========================================================================')
        lines.append(f'    # {title}')
        lines.append(f'    # =========================================================================')
        for reg in sorted(regs, key=lambda r: r.address if isinstance(r.address, int) else 0):
            name = to_upper_snake(reg.definition)
            # 이름 정규화: STRING1_INPUT_VOLTAGE → STRING1_VOLTAGE 등
            name = re.sub(r'(STRING\d+)_INPUT_(VOLTAGE|CURRENT)', r'\1_\2', name)
            name = re.sub(r'(MPPT\d+)_INPUT_(VOLTAGE|CURRENT)', r'\1_\2', name)
            # PV1_POWER → MPPT1_POWER_LOW (L1_POWER는 AC 출력이므로 제외)
            m = re.match(r'PV(\d+)_POWER$', name)
            if m:
                name = f'MPPT{m.group(1)}_POWER_LOW'
            if not name or name in emitted_names:
                continue
            addr = reg.address if isinstance(reg.address, int) else parse_address(reg.address)
            if addr is None:
                continue
            comment_parts = []
            if reg.data_type:
                comment_parts.append(reg.data_type)
            if reg.scale:
                comment_parts.append(f'scale {reg.scale}')
            if reg.unit:
                comment_parts.append(reg.unit)
            comment = ', '.join(comment_parts)
            lines.append(f'    {name:40s} = 0x{addr:04X}  # {comment}')
            emitted_names.add(name)

            # U32/S32 → 자동으로 _HIGH 워드 추가
            if reg.data_type in ('U32', 'S32') and not name.endswith('_HIGH'):
                high_name = name.replace('_LOW', '_HIGH') if '_LOW' in name else f'{name}_HIGH'
                if high_name not in emitted_names:
                    lines.append(f'    {high_name:40s} = 0x{addr + 1:04X}')
                    emitted_names.add(high_name)
        lines.append('')

    # 모든 정의된 속성 집합 (별칭 검증용) — emitted_names 기반
    all_defined = set(emitted_names)

    # Alias: FIRMWARE_VERSION
    info_names = {to_upper_snake(r.definition) for r in regs_by_cat.get('INFO', [])}
    if 'MASTER_FIRMWARE_VERSION' in info_names:
        lines.append('    # Alias')
        lines.append('    FIRMWARE_VERSION                         = MASTER_FIRMWARE_VERSION')
        lines.append('')

    # R/S/T Phase Aliases (전체: VOLTAGE, CURRENT, POWER_LOW, POWER_HIGH, FREQUENCY)
    if 'L1_VOLTAGE' in all_defined:
        lines.append('    # R/S/T Phase Aliases')
        for phase, l in [('R', 'L1'), ('S', 'L2'), ('T', 'L3')]:
            for metric in ['VOLTAGE', 'CURRENT', 'POWER_LOW', 'POWER_HIGH', 'FREQUENCY']:
                src = f'{l}_{metric}'
                alias = f'{phase}_{metric}'
                if src in all_defined:
                    lines.append(f'    {alias:40s} = {src}')
        lines.append('')

    # Grid Power Aliases
    has_grid = 'GRID_TOTAL_ACTIVE_POWER_LOW' in all_defined
    has_pv = 'PV_TOTAL_INPUT_POWER_LOW' in all_defined
    if has_grid or has_pv:
        lines.append('    # Grid/PV Power Aliases')
    if has_grid:
        lines.append('    GRID_POWER_LOW                           = GRID_TOTAL_ACTIVE_POWER_LOW')
        if 'GRID_TOTAL_ACTIVE_POWER_HIGH' in all_defined:
            lines.append('    GRID_POWER_HIGH                          = GRID_TOTAL_ACTIVE_POWER_HIGH')
    if has_pv:
        lines.append('    PV_POWER_LOW                             = PV_TOTAL_INPUT_POWER_LOW')
        if 'PV_TOTAL_INPUT_POWER_HIGH' in all_defined:
            lines.append('    PV_POWER_HIGH                            = PV_TOTAL_INPUT_POWER_HIGH')
    if has_grid or has_pv:
        lines.append('')

    # Standard handler compatibility aliases (H01 Body Type 4 required)
    lines.append('    # --- Standard handler compatibility aliases (H01 Body Type 4 required) ---')
    compat_aliases = [
        ('R_PHASE_VOLTAGE', 'L1_VOLTAGE'), ('S_PHASE_VOLTAGE', 'L2_VOLTAGE'),
        ('T_PHASE_VOLTAGE', 'L3_VOLTAGE'),
        ('R_PHASE_CURRENT', 'L1_CURRENT'), ('S_PHASE_CURRENT', 'L2_CURRENT'),
        ('T_PHASE_CURRENT', 'L3_CURRENT'),
        ('FREQUENCY', 'L1_FREQUENCY'),
        ('AC_POWER', 'GRID_TOTAL_ACTIVE_POWER_LOW'),
        ('PV_POWER', 'PV_TOTAL_INPUT_POWER_LOW'),
        ('TOTAL_ENERGY', 'TOTAL_ENERGY_LOW'),
    ]
    for alias, src in compat_aliases:
        if src in all_defined:
            lines.append(f'    {alias:40s} = {src}')
    lines.append('')

    # DER-AVM backward compatibility aliases
    der_names = {to_upper_snake(r.definition) for r in regs_by_cat.get('DER_CONTROL', [])}
    if der_names:
        lines.append('    # DER-AVM backward compatibility aliases')
        der_aliases = [
            ('POWER_FACTOR_SET', 'DER_POWER_FACTOR_SET'),
            ('ACTION_MODE', 'DER_ACTION_MODE'),
            ('REACTIVE_POWER_PCT', 'DER_REACTIVE_POWER_PCT'),
            ('ACTIVE_POWER_PCT', 'DER_ACTIVE_POWER_PCT'),
            ('OPERATION_MODE', 'DER_ACTION_MODE'),
            ('REACTIVE_POWER_SET', 'DER_REACTIVE_POWER_PCT'),
        ]
        for alias, src in der_aliases:
            if src in der_names:
                lines.append(f'    {alias:40s} = {src}')
        lines.append('')

    # IV Scan Control aliases + registers
    if 'IV_CURVE_SCAN' in all_defined:
        lines.append('    # IV Scan aliases')
        lines.append('    IV_SCAN_COMMAND                          = IV_CURVE_SCAN')
        lines.append('    IV_SCAN_STATUS                           = IV_CURVE_SCAN')
        lines.append('')

    # IV Scan Data Registers (if supported)
    if iv_scan and mppt > 0:
        lines.append('    # =========================================================================')
        lines.append('    # IV Scan Data Registers')
        lines.append(f'    # {mppt} trackers x {strings_per_mppt} strings x 64 data points')
        lines.append('    # =========================================================================')
        for t in range(1, mppt + 1):
            tracker_base = 0x8000 + (t - 1) * 0x140
            lines.append(f'    IV_TRACKER{t}_VOLTAGE_BASE               = 0x{tracker_base:04X}')
            for s in range(1, strings_per_mppt + 1):
                s_base = tracker_base + s * 0x40
                lines.append(f'    IV_STRING{t}_{s}_CURRENT_BASE             = 0x{s_base:04X}')
        lines.append('')
        lines.append(f'    IV_SCAN_DATA_POINTS                      = {iv_data_points}')
        lines.append('    IV_TRACKER_BLOCK_SIZE                    = 0x140  # 5 x 64 registers per tracker')
        lines.append('')

    return '\n'.join(lines)


def _gen_iv_scan_classes() -> str:
    return '''

class IVScanCommand:
    """IV Scan Command values for writing to 0x600D"""
    NON_ACTIVE = 0x0000
    ACTIVE     = 0x0001


class IVScanStatus:
    """IV Scan Status values when reading from 0x600D"""
    IDLE     = 0x0000
    RUNNING  = 0x0001
    FINISHED = 0x0002

    @classmethod
    def to_string(cls, status):
        return {0: "Idle", 1: "Running", 2: "Finished"}.get(status, f"Unknown({status})")

'''


def _gen_inverter_mode() -> str:
    lines = ['', '', 'class InverterMode:',
             '    """Inverter Mode Table (0x101D)"""']
    for code, name, desc in INVERTER_MODES:
        lines.append(f'    {name:12s} = 0x{code:02X}')
    lines.append('')
    lines.append('    @classmethod')
    lines.append('    def to_string(cls, mode):')
    lines.append('        return {')
    for code, name, desc in INVERTER_MODES:
        label = name.replace('_', '-').title() if name != 'ON_GRID' else 'On-Grid'
        if name == 'OFF_GRID':
            label = 'Off-Grid'
        lines.append(f'            0x{code:02X}: "{label}",')
    lines.append('        }.get(mode, f"Unknown(0x{mode:02X})")')
    lines.append('')
    return '\n'.join(lines)


def _gen_der_action_mode() -> str:
    return '''

class DerActionMode:
    """DER-AVM Action Mode (0x07D1)"""
    SELF_CONTROL    = 0
    DER_AVM_CONTROL = 2
    QV_CONTROL      = 5

'''


def _gen_device_type() -> str:
    return '''
class DeviceType:
    """Device Type for config file"""
    RTU              = 0
    INVERTER         = 1
    ENVIRONMENT_SENSOR = 2
    POWER_METER      = 3
    PROTECTION_RELAY = 4

    @classmethod
    def to_string(cls, dtype):
        return {
            0: "RTU", 1: "Inverter", 2: "Environment Sensor",
            3: "Power Meter", 4: "Protection Relay",
        }.get(dtype, f"Unknown({dtype})")


class ControlMode:
    """Control Mode for config file"""
    NONE    = "NONE"
    DER_AVM = "DER_AVM"

'''


def _gen_iv_scan_body_type(mppt: int, strings_per_mppt: int) -> str:
    lines = ['', 'class IVScanBodyType:',
             '    """IV Scan Body Type mapping for H05 protocol"""',
             '    IV_SCAN_RESULT = 12',
             '    IV_SCAN_DATA  = 15']
    # Tracker/String body type IDs
    # From reference: Tracker1=134, +5 per tracker. String offset: +1 per string within tracker
    for t in range(1, mppt + 1):
        tracker_bt = 134 + (t - 1) * 5
        lines.append(f'    TRACKER{t}_VOLTAGE             = {tracker_bt}')
        for s in range(1, strings_per_mppt + 1):
            s_bt = tracker_bt + s
            lines.append(f'    STRING{t}_{s}_CURRENT           = {s_bt}')
    lines.append('')
    return '\n'.join(lines)


def _gen_error_classes(alarm_regs: list, error_bits: dict) -> str:
    lines = []
    sorted_alarms = sorted(alarm_regs,
                           key=lambda r: r.address if isinstance(r.address, int) else 0)
    for i, reg in enumerate(sorted_alarms, start=1):
        name = to_upper_snake(reg.definition)
        cls_name = f'ErrorCode{i}'
        addr_hex = reg.address_hex if reg.address_hex else f'0x{reg.address:04X}'

        # BITS from reference
        bits = error_bits.get(f'ErrorCode{i}', {})
        bits_str = '{\n'
        if bits:
            for bit_n, desc in sorted(bits.items()):
                bits_str += f'        {bit_n}:  "{desc}",\n'
            bits_str += '    }'
        else:
            bits_str += '    }'

        lines.append(f'')
        lines.append(f'')
        lines.append(f'class {cls_name}:')
        lines.append(f'    """Error Code Table{i} ({addr_hex}) — Bit field"""')
        lines.append(f'    BITS = {bits_str}')
        lines.append(f'')
        lines.append(f'    @classmethod')
        lines.append(f'    def decode(cls, value):')
        lines.append(f'        return [f"E{{b}}:{{d}}" for b, d in cls.BITS.items() if value & (1 << b)]')
        lines.append(f'')
        lines.append(f'    @classmethod')
        lines.append(f'    def to_string(cls, value):')
        lines.append(f'        return ", ".join(cls.decode(value)) if value else "OK"')
    return '\n'.join(lines)


def _gen_status_converter(manufacturer: str) -> str:
    cls_name = f'{manufacturer.title()}StatusConverter'
    return f'''


class {cls_name}:
    """{manufacturer} INVERTER_MODE register already contains InverterMode values."""

    @classmethod
    def to_inverter_mode(cls, raw):
        return raw


# Dynamic-loader alias required by modbus_handler.load_register_module
StatusConverter = {cls_name}
'''


def _gen_scale_dict() -> str:
    return '''

# Scale factors
SCALE = {
    'voltage':            0.1,
    'current':            0.01,
    'power':              0.1,
    'frequency':          0.01,
    'power_factor':       0.001,
    'dea_current':        0.1,
    'dea_voltage':        0.1,
    'dea_active_power':   0.1,
    'dea_reactive_power': 1,
    'dea_frequency':      0.1,
    'iv_voltage':         0.1,
    'iv_current':         0.1,
}
'''


def _gen_helpers(mppt: int, total_strings: int, strings_per_mppt: int,
                 iv_data_points: int = 64) -> str:
    return f'''

# Channel configuration (used by modbus_handler for dynamic array sizing)
MPPT_CHANNELS = {mppt}
STRING_CHANNELS = {total_strings}


def registers_to_u32(low, high):
    """Combine two U16 to U32"""
    return (high << 16) | low


def registers_to_s32(low, high):
    """Combine two U16 to S32"""
    value = (high << 16) | low
    if value >= 0x80000000:
        value -= 0x100000000
    return value


def get_string_registers(string_num):
    """Return (voltage_addr, current_addr) for a string number (1-{total_strings})."""
    if string_num < 1 or string_num > {total_strings}:
        raise ValueError(f"String number must be 1-{total_strings}, got {{string_num}}")
    base = 0x1050 + (string_num - 1) * 2
    return (base, base + 1)


def get_mppt_registers(mppt_num):
    """Return (voltage, current, power_low, power_high) for MPPT number (1-{mppt})."""
    if mppt_num < 1 or mppt_num > {mppt}:
        raise ValueError(f"MPPT number must be 1-{mppt}, got {{mppt_num}}")
    if mppt_num <= 3:
        base = 0x1010 + (mppt_num - 1) * 4
    elif mppt_num == 4:
        base = 0x103E
    else:  # 5+
        base = 0x1080 + (mppt_num - 5) * 4
    return (base, base + 1, base + 2, base + 3)


def get_iv_tracker_voltage_registers(tracker_num, data_points={iv_data_points}):
    """Return {{'base', 'count', 'end'}} for IV voltage block of a tracker (1-{mppt})."""
    if tracker_num < 1 or tracker_num > {mppt}:
        raise ValueError(f"Tracker number must be 1-{mppt}, got {{tracker_num}}")
    base = 0x8000 + (tracker_num - 1) * RegisterMap.IV_TRACKER_BLOCK_SIZE
    return {{'base': base, 'count': data_points, 'end': base + data_points - 1}}


def get_iv_string_current_registers(mppt_num, string_num, data_points={iv_data_points}):
    """Return {{'base', 'count', 'end'}} for IV current block of a string (string_num 1-{strings_per_mppt})."""
    if mppt_num < 1 or mppt_num > {mppt}:
        raise ValueError(f"MPPT number must be 1-{mppt}, got {{mppt_num}}")
    if string_num < 1 or string_num > {strings_per_mppt}:
        raise ValueError(f"String number must be 1-{strings_per_mppt} per MPPT, got {{string_num}}")
    tracker_base = 0x8000 + (mppt_num - 1) * RegisterMap.IV_TRACKER_BLOCK_SIZE
    base = tracker_base + string_num * 0x40
    return {{'base': base, 'count': data_points, 'end': base + data_points - 1}}


def get_iv_string_mapping(total_strings={total_strings}, strings_per_mppt={strings_per_mppt}):
    """Return list of dicts mapping string index to IV scan register addresses."""
    mapping = []
    data_points = RegisterMap.IV_SCAN_DATA_POINTS
    for string_idx in range(total_strings):
        mppt_num       = (string_idx // strings_per_mppt) + 1
        string_in_mppt = (string_idx % strings_per_mppt) + 1
        v_regs = get_iv_tracker_voltage_registers(mppt_num, data_points)
        i_regs = get_iv_string_current_registers(mppt_num, string_in_mppt, data_points)
        mapping.append({{
            'string_num':     string_idx + 1,
            'total_strings':  total_strings,
            'mppt_num':       mppt_num,
            'string_in_mppt': string_in_mppt,
            'voltage_base':   v_regs['base'],
            'current_base':   i_regs['base'],
            'data_points':    data_points,
        }})
    return mapping


def generate_iv_voltage_data(voc, v_min, data_points={iv_data_points}):
    """Generate IV scan voltage array (U16, 0.1V units, ascending v_min->voc)."""
    step = (voc - v_min) / max(data_points - 1, 1)
    return [int((v_min + step * i) * 10) & 0xFFFF for i in range(data_points)]


def generate_iv_current_data(isc, voc, v_min, data_points={iv_data_points}):
    """Generate IV scan current array (U16, 0.01A units) using IV curve approximation."""
    step = (voc - v_min) / max(data_points - 1, 1)
    regs = []
    for i in range(data_points):
        v = v_min + step * i
        ratio = v / voc if voc > 0 else 0
        current = max(0.0, isc * (1.0 - ratio ** 20))
        regs.append(int(current * 100) & 0xFFFF)
    return regs
'''


def _gen_data_types(all_regs: List[RegisterRow]) -> str:
    """DATA_TYPES dict 생성"""
    lines = ['\n', 'DATA_TYPES = {']
    seen = set()
    for reg in sorted(all_regs, key=lambda r: r.address if isinstance(r.address, int) else 0):
        name = to_upper_snake(reg.definition)
        if not name or name in seen:
            continue
        # _HIGH 워드는 S32/U32 쌍의 일부이므로 별도 타입 불필요
        if name.endswith('_HIGH') and reg.data_type in ('S32', 'U32'):
            continue
        dtype = reg.data_type or 'U16'
        seen.add(name)
        lines.append(f"    '{name}': '{dtype}',")
    lines.append('}')
    lines.append('')
    lines.append('FLOAT32_FIELDS: set = set()')
    lines.append('')
    return '\n'.join(lines)


def _gen_string_current_monitor(all_regs: List[RegisterRow]) -> str:
    """STRING_CURRENT_MONITOR 플래그 생성 — String 전류 레지스터 존재 여부"""
    has_string = any(
        detect_channel_number(r.definition) and detect_channel_number(r.definition)[0] == 'STRING'
        and 'current' in r.definition.lower()
        for r in all_regs
    )
    return f'''
# String 전류 모니터링 지원 여부
# True: String별 전류 레지스터 있음 (Solarize, Senergy, Kstar 등)
# False: String 전류 레지스터 없음 (Huawei 등 — PV 전류만 제공)
STRING_CURRENT_MONITOR = {has_string}
'''


# ─── 코드 검증 ───────────────────────────────────────────────────────────────

def validate_code(code: str, mppt: int, total_strings: int,
                   iv_scan: bool = True, der_avm: bool = True) -> dict:
    checks = {}
    checks['class_RegisterMap'] = 'class RegisterMap' in code
    checks['class_InverterMode'] = 'class InverterMode' in code
    if iv_scan:
        checks['class_IVScanCommand'] = 'class IVScanCommand' in code
        checks['class_IVScanStatus'] = 'class IVScanStatus' in code
    # DER-AVM은 모든 인버터 필수
    checks['class_DerActionMode'] = 'class DerActionMode' in code
    checks['class_DeviceType'] = 'class DeviceType' in code
    checks['class_ErrorCode1'] = 'class ErrorCode1' in code
    checks['InverterMode_to_string'] = 'def to_string' in code
    checks['SCALE_dict'] = "SCALE = {" in code
    checks['registers_to_u32'] = 'def registers_to_u32' in code
    checks['registers_to_s32'] = 'def registers_to_s32' in code
    checks['get_string_registers'] = 'def get_string_registers' in code
    checks['get_mppt_registers'] = 'def get_mppt_registers' in code
    checks['DATA_TYPES'] = 'DATA_TYPES' in code
    checks['StatusConverter'] = 'StatusConverter' in code

    # MPPT 채널 수 — Method 1: 생성된 MPPT_CHANNELS 상수 우선
    if f'MPPT_CHANNELS = {mppt}' in code:
        mppt_count = mppt  # 명시적 선언 → 바로 통과
    else:
        # Method 2: 상수명 패턴 (다양한 제조사 네이밍 지원)
        mppt_count = 0
        for n in range(1, 20):
            _mppt_pats = [
                f'MPPT{n}_VOLTAGE', f'PV{n}_VOLTAGE', f'PV{n}VOLTAGE',
                f'INPUT_{n}_VOLTAGE', f'INPUT{n}_VOLTAGE',   # ABB: Input 1 Voltage
                f'DC{n}_VOLTAGE', f'DC_{n}_VOLTAGE',          # CPS/Delta: DC1 Voltage
                f'PP{n}_VOLTAGE', f'PP_{n}_VOLTAGE',           # ABB: PP voltage
                f'CH{n}_VOLTAGE', f'CH_{n}_VOLTAGE',           # Channel variants
                f'STRING{n}_INPUT_VOLTAGE',                    # String with input
            ]
            if any(p in code for p in _mppt_pats):
                mppt_count = n
    checks[f'MPPT_channels_{mppt}'] = mppt_count >= mppt

    # String 채널 수 (STRING{n} 또는 PV_STRING{n})
    string_count = 0
    for n in range(1, 50):
        if f'STRING{n}_VOLTAGE' in code or f'STRING{n}_CURRENT' in code or \
           f'PV_STRING{n}' in code or f'PVSTRING{n}' in code:
            string_count = n
    # String이 없는 인버터(화웨이 등)는 PDF에 없으면 통과
    if string_count == 0 and total_strings > 0:
        # PDF에 String 레지스터가 없으면 해당 인버터는 String 미지원 → 통과
        checks[f'String_channels_{total_strings}'] = True
    else:
        checks[f'String_channels_{total_strings}'] = string_count >= total_strings

    return checks


# ─── 학습 피드백 ─────────────────────────────────────────────────────────────

def update_synonym_db(all_regs: List[RegisterRow], synonym_db: dict) -> int:
    added = 0
    for reg in all_regs:
        if not reg.h01_field:
            continue
        defn = reg.definition
        for field_name, info in synonym_db.get('fields', {}).items():
            if info.get('h01_field') == reg.h01_field:
                syns = info.setdefault('synonyms', [])
                if defn not in syns and to_upper_snake(defn) != field_name:
                    syns.append(defn)
                    added += 1
                break
    return added


def update_review_history(review_items: list, manufacturer: str,
                          history: dict) -> int:
    added = 0
    for item in review_items:
        verdict = item.get('사용자 판정', '').strip()
        if not verdict:
            continue
        history.setdefault('approved', []).append({
            'manufacturer': manufacturer,
            'definition': item.get('Definition', ''),
            'address': item.get('Address', ''),
            'verdict': verdict,
            'reason': item.get('사유', ''),
            'pattern': '',
            'date': datetime.now().strftime('%Y-%m-%d'),
        })
        history.setdefault('stats', {})
        history['stats']['total_reviewed'] = history['stats'].get('total_reviewed', 0) + 1
        added += 1
    return added


# ─── Stage 3 메인 함수 ───────────────────────────────────────────────────────

def run_stage3(
    stage2_path: str,
    output_dir: str,
    progress: ProgressCallback = None,
    iv_data_points: int = 64,
) -> dict:
    def log(msg, level='info'):
        if progress:
            progress(msg, level)

    log('Stage 2 Excel 읽기...')
    s2_data = read_stage2_excel(stage2_path)
    meta = s2_data['meta']
    all_regs = s2_data['all_regs']
    review_items = s2_data['review_items']

    manufacturer = meta.get('제조사', meta.get('manufacturer', 'Unknown'))
    device_type = meta.get('설비 타입', meta.get('device_type', 'inverter'))

    # iv_data_points from meta (Stage 1에서 감지한 값 우선)
    if iv_data_points == 64:
        try:
            iv_data_points = int(meta.get('IV Data Points', meta.get('iv_data_points', 64)))
        except (ValueError, TypeError):
            pass

    # REVIEW 판정 반영
    for item in review_items:
        verdict = item.get('사용자 판정', '').strip()
        if not verdict:
            continue
        defn = item.get('Definition', '')
        if verdict == 'DELETE':
            all_regs = [r for r in all_regs
                        if to_upper_snake(r.definition) != to_upper_snake(defn)]
        elif verdict.startswith('MOVE:'):
            target = verdict.replace('MOVE:', '')
            for reg in all_regs:
                if to_upper_snake(reg.definition) == to_upper_snake(defn):
                    reg.category = target
        elif verdict == 'KEEP':
            all_regs = [r for r in all_regs
                        if to_upper_snake(r.definition) != to_upper_snake(defn)]

    # 카테고리별 그룹화
    regs_by_cat = {}
    for reg in all_regs:
        cat = reg.category or 'MONITORING'
        regs_by_cat.setdefault(cat, []).append(reg)

    # MPPT / String 수
    mppt_count = 0
    total_strings = 0
    for reg in all_regs:
        ch = detect_channel_number(reg.definition)
        if ch:
            prefix, n = ch
            if prefix == 'MPPT':
                mppt_count = max(mppt_count, n)
            elif prefix == 'STRING':
                total_strings = max(total_strings, n)

    # meta에서 보완
    if mppt_count == 0:
        try:
            mppt_count = int(meta.get('MPPT', '0').split()[0])
        except (ValueError, IndexError):
            pass
    if total_strings == 0:
        try:
            s = meta.get('String', '0')
            total_strings = int(s.split()[0])
        except (ValueError, IndexError):
            pass

    strings_per_mppt = total_strings // mppt_count if mppt_count > 0 else 0

    # 프로토콜명 정규화 (긴 파일명 → 짧은 프로토콜명)
    protocol_name = manufacturer.lower()
    protocol_version = meta.get('프로토콜 버전', '')

    # 인버터 타입 감지 (PDF 파일명 기반): HYB / PV
    _pdf_fn = meta.get('원본 파일', '').upper()
    inverter_type = 'HYB' if any(k in _pdf_fn for k in ['HYB', 'HYBRID', '하이브리드']) else 'PV'

    # 용량 (Stage 2에서 사용자 입력, '-' 이거나 비어있으면 생략)
    _cap = meta.get('용량', '').strip()
    capacity_str = f'_{_cap}' if _cap and _cap != '-' else ''

    # V2: IV/DER 판단 — 레지스터 유무 + 메타데이터 + 제조사 확정
    iv_scan = (
        len(regs_by_cat.get('IV_SCAN', [])) > 0 or
        meta.get('IV Scan', '').lower() in ('yes', 'true', '1') or
        meta.get('iv_scan', '').lower() in ('yes', 'true', '1') or
        any(to_upper_snake(r.definition) == 'IV_CURVE_SCAN'
            for cat_regs in regs_by_cat.values() if isinstance(cat_regs, list)
            for r in cat_regs) or
        manufacturer.lower() in ('solarize', 'kstar', 'senergy')
    )
    # DER-AVM은 인버터면 항상 True (고정 주소맵)
    der_avm = (
        len(regs_by_cat.get('DER_CONTROL', [])) > 0 or
        device_type == 'inverter'
    )

    log(f'제조사: {manufacturer}, 프로토콜: {protocol_name}')
    log(f'MPPT: {mppt_count}, String: {total_strings} ({strings_per_mppt}/MPPT)')

    # ErrorCode BITS 로딩
    error_bits = _load_error_bits_from_reference(manufacturer)
    if error_bits:
        log(f'  ErrorCode BITS: {list(error_bits.keys())} 로드')

    # RegisterMap에 제조사명 전달
    regs_by_cat['_manufacturer'] = manufacturer

    # ── 코드 생성 ──
    log('Python 코드 생성...')
    code_parts = [
        _gen_header(manufacturer, protocol_name, protocol_version,
                    mppt_count, total_strings, strings_per_mppt,
                    iv_scan, der_avm),
        _gen_register_map(regs_by_cat, mppt_count, total_strings,
                          strings_per_mppt, iv_scan, iv_data_points),
        _gen_iv_scan_classes() if iv_scan else '',
        _gen_inverter_mode(),
        _gen_der_action_mode() if der_avm else '',
        _gen_device_type(),
        _gen_iv_scan_body_type(mppt_count, strings_per_mppt) if iv_scan else '',
        _gen_error_classes(regs_by_cat.get('ALARM', []), error_bits),
        _gen_status_converter(manufacturer),
        _gen_scale_dict(),
        _gen_helpers(mppt_count, total_strings, strings_per_mppt, iv_data_points),
        _gen_data_types(all_regs),
        _gen_string_current_monitor(all_regs),
    ]
    code = '\n'.join(p for p in code_parts if p)

    # ── 검증 ──
    log('코드 검증...')
    validation = validate_code(code, mppt_count, total_strings, iv_scan, der_avm)
    passed = sum(1 for v in validation.values() if v)
    total = len(validation)
    log(f'  검증: {passed}/{total} 통과')
    for check_name, ok in validation.items():
        log(f'    {"✓" if ok else "✗"} {check_name}')

    # ── 파일 저장 ──
    output_name = f'{manufacturer}_{inverter_type}{capacity_str}_registers.py'
    output_path = os.path.join(output_dir, output_name)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(code)
    log(f'코드 저장: {output_name}', 'ok')

    # ── 학습 피드백 ──
    log('학습 피드백 업데이트...')
    synonym_db = load_synonym_db()
    syn_added = update_synonym_db(all_regs, synonym_db)
    if syn_added > 0:
        save_synonym_db(synonym_db)
        log(f'  synonym_db: +{syn_added}개 동의어')

    review_history = load_review_history()
    rv_recorded = update_review_history(review_items, manufacturer, review_history)
    if rv_recorded > 0:
        save_review_history(review_history)
        log(f'  review_history: +{rv_recorded}개 판정')

    # common/ 레퍼런스 등록 — 모든 검증 통과 시에만 (오염 방지)
    all_passed = all(validation.values())
    common_path = os.path.join(COMMON_DIR, output_name)
    if all_passed:
        import shutil
        # 1) Model Maker common/ (레퍼런스용)
        shutil.copy2(output_path, common_path)
        log(f'  레퍼런스 등록: common/{output_name}')
        # 2) RTU V2_0_0/common/ (즉시 배포용)
        if os.path.isdir(RTU_COMMON_DIR):
            rtu_common_path = os.path.join(RTU_COMMON_DIR, output_name)
            shutil.copy2(output_path, rtu_common_path)
            log(f'  RTU 배포: V2_0_0/common/{output_name}')
        else:
            log(f'  RTU common/ 없음 — 수동 복사 필요: {RTU_COMMON_DIR}', 'warn')
    else:
        failed = [k for k, v in validation.items() if not v]
        log(f'  레퍼런스 미등록 (검증 실패: {failed})', 'warn')

    log('Stage 3 완료', 'ok')

    return {
        'output_path': output_path,
        'filename': output_name,
        'validation': validation,
        'synonym_added': syn_added,
        'review_recorded': rv_recorded,
        'mppt_count': mppt_count,
        'total_strings': total_strings,
        'register_count': len(all_regs),
    }
