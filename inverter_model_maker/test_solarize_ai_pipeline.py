# -*- coding: utf-8 -*-
"""
Test: Solarize AI Pipeline (Stage 1->2->3) and compare with manual register file.
"""
import sys, os, time, json, shutil, re

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'model_maker_web_v2', 'backend'))
sys.path.insert(0, os.path.dirname(__file__))

from pipeline.stage1 import run_stage1
from pipeline.stage2 import run_stage2
from pipeline.stage3 import run_stage3

PDF_PATH = os.path.join(os.path.dirname(__file__),
    'model_maker_web_v2', 'results', 'Solarize',
    'Solarize-PV_Modbus_Protocol-Korea-V1.2.4.pdf')

MANUAL_FILE = os.path.join(os.path.dirname(__file__), '..',
    'common', 'Solarize_50_3_MPPT4_STR8_registers.py')

# AI settings — read from config/ai_settings.ini
def _load_ai_settings():
    import configparser
    ini = os.path.join(os.path.dirname(__file__), '..', 'config', 'ai_settings.ini')
    cp = configparser.ConfigParser()
    cp.read(ini, encoding='utf-8')
    return {
        'api_key': cp.get('claude_api', 'api_key', fallback=''),
        'model': cp.get('claude_api', 'model', fallback='claude-sonnet-4-20250514'),
    }
AI_SETTINGS = _load_ai_settings()

def log_cb(msg, level='info'):
    try:
        print(f'  [{level}] {msg}')
    except UnicodeEncodeError:
        print(f'  [{level}] {msg.encode("ascii", "replace").decode()}')

def extract_register_names(py_path):
    """Extract RegisterMap constant names from a _registers.py file."""
    names = set()
    in_class = False
    with open(py_path, 'r', encoding='utf-8') as f:
        for line in f:
            stripped = line.strip()
            if stripped.startswith('class RegisterMap'):
                in_class = True
                continue
            if in_class and stripped and not stripped.startswith('#'):
                if stripped.startswith('class ') or stripped.startswith('def '):
                    in_class = False
                    continue
                # Match: NAME = 0xNNNN or NAME = OTHER_NAME
                m = re.match(r'^(\w+)\s*=\s*', stripped)
                if m:
                    names.add(m.group(1))
    return names

def extract_error_bits(py_path):
    """Extract ErrorCode class bit counts."""
    bits = {}
    current_class = None
    bit_count = 0
    with open(py_path, 'r', encoding='utf-8') as f:
        for line in f:
            stripped = line.strip()
            m = re.match(r'^class (ErrorCode\d+)', stripped)
            if m:
                if current_class:
                    bits[current_class] = bit_count
                current_class = m.group(1)
                bit_count = 0
                continue
            if current_class and re.match(r'^\d+\s*:', stripped):
                bit_count += 1
    if current_class:
        bits[current_class] = bit_count
    return bits

def main():
    if not AI_SETTINGS['api_key']:
        print('ERROR: ANTHROPIC_API_KEY not set')
        sys.exit(1)

    if not os.path.exists(PDF_PATH):
        print(f'ERROR: PDF not found: {PDF_PATH}')
        sys.exit(1)

    # Work directory
    work_dir = os.path.join(os.path.dirname(__file__), 'test_ai_output_solarize')
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
    os.makedirs(work_dir)

    print(f'=== Solarize AI Pipeline Test ===')
    print(f'PDF: {os.path.basename(PDF_PATH)}')
    print(f'Output: {work_dir}')
    print()

    # ── Stage 1: AI PDF extraction ──
    print('── Stage 1: AI PDF Extraction ──')
    t0 = time.time()
    s1_result = run_stage1(PDF_PATH, work_dir, 'inverter', log_cb, ai_settings=AI_SETTINGS)
    t1 = time.time()
    print(f'  Stage 1 done: {t1-t0:.1f}s')
    print(f'  Output: {s1_result.get("output_file", "?")}')
    print()

    # ── Stage 2: MPPT/String filtering ──
    s1_output = s1_result.get('output_file', '')
    if not s1_output or not os.path.exists(s1_output):
        # Find the stage1 xlsx
        for f in os.listdir(work_dir):
            if 'stage1' in f.lower() and f.endswith('.xlsx'):
                s1_output = os.path.join(work_dir, f)
                break

    print('── Stage 2: MPPT/String Filtering ──')
    t2 = time.time()
    s2_result = run_stage2(s1_output, work_dir, mppt_count=4, total_strings=8,
                           capacity='50', progress=log_cb)
    t3 = time.time()
    print(f'  Stage 2 done: {t3-t2:.1f}s')
    print()

    # ── Stage 3: Code Generation ──
    s2_output = s2_result.get('output_file', '')
    if not s2_output or not os.path.exists(s2_output):
        for f in os.listdir(work_dir):
            if 'stage2' in f.lower() and f.endswith('.xlsx'):
                s2_output = os.path.join(work_dir, f)
                break

    print('── Stage 3: Code Generation ──')
    t4 = time.time()
    s3_result = run_stage3(s2_output, work_dir, log_cb)
    t5 = time.time()
    print(f'  Stage 3 done: {t5-t4:.1f}s')
    print()

    # Find generated .py file
    gen_file = None
    for f in os.listdir(work_dir):
        if f.endswith('_registers.py'):
            gen_file = os.path.join(work_dir, f)
            break

    if not gen_file:
        print('ERROR: No _registers.py generated!')
        sys.exit(1)

    print(f'Generated: {os.path.basename(gen_file)}')
    print(f'Manual:    {os.path.basename(MANUAL_FILE)}')
    print()

    # ── Compare ──
    print('═══ COMPARISON ═══')
    gen_names = extract_register_names(gen_file)
    manual_names = extract_register_names(MANUAL_FILE)

    # Exclude _HIGH for fair comparison of unique registers
    gen_core = {n for n in gen_names if not n.endswith('_HIGH')}
    manual_core = {n for n in manual_names if not n.endswith('_HIGH')}

    print(f'Generated RegisterMap constants: {len(gen_names)} ({len(gen_core)} excl _HIGH)')
    print(f'Manual RegisterMap constants:    {len(manual_names)} ({len(manual_core)} excl _HIGH)')
    print()

    # Common
    common = gen_core & manual_core
    only_gen = gen_core - manual_core
    only_manual = manual_core - gen_core

    print(f'Common:       {len(common)}')
    print(f'Only in AI:   {len(only_gen)}')
    print(f'Only in Manual: {len(only_manual)}')
    print()

    if only_gen:
        print('── Only in AI-generated (not in manual) ──')
        for n in sorted(only_gen):
            print(f'  + {n}')
        print()

    if only_manual:
        print('── Only in Manual (missing from AI) ──')
        for n in sorted(only_manual):
            print(f'  - {n}')
        print()

    # ErrorCode BITS comparison
    gen_bits = extract_error_bits(gen_file)
    manual_bits = extract_error_bits(MANUAL_FILE)
    print('── ErrorCode BITS ──')
    all_ec = sorted(set(list(gen_bits.keys()) + list(manual_bits.keys())))
    for ec in all_ec:
        gb = gen_bits.get(ec, 0)
        mb = manual_bits.get(ec, 0)
        match = '✓' if gb == mb else '✗'
        print(f'  {ec}: AI={gb} Manual={mb} {match}')
    print()

    # Match percentage
    if manual_core:
        coverage = len(common) / len(manual_core) * 100
        print(f'Coverage: {coverage:.1f}% of manual registers found by AI pipeline')

    total_time = t5 - t0
    print(f'Total time: {total_time:.1f}s')

if __name__ == '__main__':
    main()
