# -*- coding: utf-8 -*-
"""
MM2 Batch Runner — runs Stage1->2->3 for each target inverter, captures
generated register file into temp/mm2_runs/{name}/, then diffs against
the verified target in common/{name}_50_3_registers.py.

Does NOT overwrite the verified targets in common/. We monkey-patch
shutil.copy2 inside stage3 to redirect deploys to temp/mm2_runs/<name>/deploys/.
"""
import os
import sys
import re
import io
import shutil
import json
import traceback

# UTF-8 stdout
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(REPO, 'inverter_model_maker', 'model_maker_web_v2'))

# After path insertion, import pipeline
from backend.pipeline.stage1 import run_stage1  # noqa: E402
from backend.pipeline.stage2 import run_stage2  # noqa: E402
from backend.pipeline.stage3 import run_stage3  # noqa: E402

OUT_BASE = os.path.join(REPO, 'temp', 'mm2_runs')
COMMON = os.path.join(REPO, 'common')
PDF_DIR = os.path.join(REPO, 'inverter_model_maker', '등록_프로토콜')

# (name, pdf_filename, mppt, total_strings, target_register_file)
INVERTERS = [
    ('Solarize_50_3', 'Solarize-PV_Modbus_Protocol-Korea-V1.2.4.pdf', 4, 8, 'Solarize_50_3_registers.py'),
    ('Senergy_50_3', 'Senergy-PV_Modbus_Protocol-Korea-V1.2.4.pdf', 4, 8, 'Senergy_50_3_registers.py'),
    ('Sungrow_50_3', 'Sungrow-PV_ti_20230117_communication-protocol_v1.1.53_en.pdf', 4, 8, 'Sungrow_50_3_registers.py'),
    ('Kstar_60_3', 'Kstar-PV_KSG1250K_Modbus_Protocol_v35.pdf', 3, 9, 'Kstar_60_3_registers.py'),
    ('Huawei_50_3', 'Huawei-PV_SUN2000MC_Modbus_Interface_Definitions.pdf', 4, 8, 'Huawei_50_3_registers.py'),
    ('Ekos_10_3', 'Ekos-PV_ModBus_map-EK_20220209_통신테스트용.xlsx', 1, 2, 'Ekos_10_3_registers.py'),
]


def parse_regs(path: str):
    """Extract NAME = 0xADDR  # comment ONLY from RegisterMap class body."""
    with open(path, encoding='utf-8') as f:
        txt = f.read()
    # Slice out the RegisterMap class body (until the next top-level class)
    m = re.search(r'^class\s+RegisterMap\b[^:]*:\s*$', txt, re.M)
    if not m:
        return {}
    start = m.end()
    m2 = re.search(r'^class\s+\w', txt[start:], re.M)
    body = txt[start:start + m2.start()] if m2 else txt[start:]
    pat = re.compile(
        r'^\s+([A-Z_][A-Z0-9_]*)\s*=\s*(0x[0-9A-Fa-f]+)\s*(?:#\s*(.*))?$', re.M)
    out = {}
    for m in pat.finditer(body):
        name, addr, com = m.groups()
        out[name] = (int(addr, 16), (com or '').strip())
    return out


def diff(target: dict, gen: dict):
    only_t = set(target) - set(gen)
    only_g = set(gen) - set(target)
    common_names = set(target) & set(gen)
    addr_diff = [n for n in common_names if target[n][0] != gen[n][0]]
    same_addr = len(common_names) - len(addr_diff)
    total = len(set(target) | set(gen))
    match_pct = round(100 * same_addr / max(1, total), 1)
    return {
        'target_count': len(target),
        'gen_count': len(gen),
        'common': len(common_names),
        'same_addr': same_addr,
        'addr_diff': addr_diff,
        'only_in_target': sorted(only_t),
        'only_in_gen': sorted(only_g),
        'match_pct': match_pct,
    }


def patch_no_deploy():
    """Patch shutil.copy2 globally so stage3's local import sees the wrap."""
    real_copy = shutil.copy2
    block_dirs = (
        os.path.normcase(COMMON),
        os.path.normcase(os.path.join(REPO, 'inverter_model_maker', 'common')),
    )

    def safe_copy(src, dst, *a, **kw):
        dst_dir = os.path.normcase(os.path.dirname(os.path.abspath(dst)))
        if dst_dir in block_dirs:
            print(f'  [skip-deploy] {dst}')
            return dst
        return real_copy(src, dst, *a, **kw)

    shutil.copy2 = safe_copy


def run_one(name, pdf, mppt, strings, target_file):
    print(f'\n========== {name} ==========')
    out = os.path.join(OUT_BASE, name)
    os.makedirs(out, exist_ok=True)
    pdf_path = os.path.join(PDF_DIR, pdf)
    if not os.path.exists(pdf_path):
        print(f'  PDF NOT FOUND: {pdf_path}')
        return None

    def log(m, lvl='info'):
        if (lvl in ('warn', 'error') or 'Stage' in m or 'PASS' in m or
                'FAIL' in m or '필터' in m or 'MONITORING' in m or
                '매칭' in m or 'enrichment' in m):
            print(f'    [{lvl}] {m}')

    try:
        r1 = run_stage1(pdf_path, out, 'inverter', log)
        s1 = r1['output_path']
        r2 = run_stage2(s1, out, mppt_count=mppt, total_strings=strings,
                        capacity=name.split('_', 1)[1], progress=log)
        s2 = r2['output_path']
        r3 = run_stage3(s2, out, progress=log)
        gen_file = r3['output_path']
    except Exception as e:
        print(f'  PIPELINE ERROR: {e}')
        traceback.print_exc()
        return {'error': str(e)}

    target_path = os.path.join(COMMON, target_file)
    if not os.path.exists(target_path):
        print(f'  TARGET NOT FOUND: {target_path}')
        return {'error': 'no target'}
    if not os.path.exists(gen_file):
        print(f'  GEN NOT FOUND: {gen_file}')
        return {'error': 'no gen'}

    target = parse_regs(target_path)
    gen = parse_regs(gen_file)
    d = diff(target, gen)
    print(f'  match: {d["match_pct"]}% '
          f'(target {d["target_count"]}, gen {d["gen_count"]}, '
          f'same_addr {d["same_addr"]}, addr_diff {len(d["addr_diff"])}, '
          f'only_t {len(d["only_in_target"])}, only_g {len(d["only_in_gen"])})')
    return d


def main():
    os.makedirs(OUT_BASE, exist_ok=True)
    patch_no_deploy()
    only = sys.argv[1:] if len(sys.argv) > 1 else None
    results = {}
    for inv in INVERTERS:
        if only and inv[0] not in only:
            continue
        d = run_one(*inv)
        results[inv[0]] = d

    summary_path = os.path.join(OUT_BASE, 'summary.json')
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f'\nWrote summary -> {summary_path}')


if __name__ == '__main__':
    main()
