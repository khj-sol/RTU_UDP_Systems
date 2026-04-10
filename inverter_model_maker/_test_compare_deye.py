# -*- coding: utf-8 -*-
"""
사용자 배치파일 환경(main 디렉토리) 동일 재현 — Deye PDF
MPPT=4, total_strings=8, capacity=50kw
"""
import os
import sys
import json
import shutil

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)
sys.path.insert(0, os.path.join(THIS_DIR, 'model_maker_web_v2'))

from model_maker_web_v2.backend.pipeline.stage1 import run_stage1
from model_maker_web_v2.backend.pipeline.stage2 import run_stage2
from model_maker_web_v2.backend.pipeline.stage3 import run_stage3, run_stage4_verification

PDF = os.path.join(
    THIS_DIR, 'model_maker_web_v2', 'results', 'Deye',
    'Deye-PV_Modbus_RTU_Protocol_V118_SUN_Series.pdf')
WORK_DIR = os.path.join(THIS_DIR, 'model_maker_web_v2', 'temp', '_compare_deye')

os.makedirs(WORK_DIR, exist_ok=True)
# 이전 산출물 정리 (input PDF 는 별도 위치라 영향 없음)
for f in os.listdir(WORK_DIR):
    fp = os.path.join(WORK_DIR, f)
    if os.path.isfile(fp):
        os.remove(fp)


def log(msg, level='info'):
    print(f'[{level}] {msg}')


def banner(t):
    print()
    print('=' * 70)
    print(f' {t}')
    print('=' * 70)


banner(f'INPUT: {os.path.basename(PDF)}')
print(f'PDF exists: {os.path.exists(PDF)}')
print(f'WORK_DIR: {WORK_DIR}')

banner('STAGE 1')
s1 = run_stage1(PDF, WORK_DIR, 'inverter', log)
print()
print(f'output: {s1.get("output_name")}')
print(f'counts: {s1.get("counts")}')
print(f'info_match: {s1.get("info_match")}')
print(f'h01_match: {s1.get("h01_match")}')
print(f'der_match: {s1.get("der_match")}')
print(f'meta phase_type: {s1.get("meta",{}).get("phase_type")}')
print(f'meta max_mppt: {s1.get("meta",{}).get("max_mppt")}')
print(f'meta max_string: {s1.get("meta",{}).get("max_string")}')
print(f'meta iv_scan: {s1.get("meta",{}).get("iv_scan")}')
print(f'meta iv_data_points: {s1.get("meta",{}).get("iv_data_points")}')

stage1_excel = s1['output_path']

banner('STAGE 2  (MPPT=4, total_strings=8, capacity=50kw)')
s2 = run_stage2(stage1_excel, WORK_DIR, mppt_count=4,
                total_strings=8, capacity='50kw', progress=log)
print()
print(f'output: {s2.get("output_name")}')
print(f'h01: {s2.get("h01_matched")}/{s2.get("h01_total")}')
print(f'der: {s2.get("der_matched")}/{s2.get("der_total")}')
print(f'stage2_pass: {s2.get("stage2_pass")}')
print(f'fail_reasons: {s2.get("fail_reasons")}')
print(f'phase_type: {s2.get("phase_type")}')
print(f'mppt_count: {s2.get("mppt_count")} total_strings: {s2.get("total_strings")}')
val = s2.get('stage2_validation', {})
if val:
    print('stage2_validation:')
    print(json.dumps(val, indent=2, ensure_ascii=False, default=str))

stage2_excel = s2['output_path']

banner('STAGE 3')
s3 = run_stage3(stage2_excel, WORK_DIR, log, iv_data_points=0)
print()
print(f'filename: {s3.get("filename")}')
print(f'phase_type: {s3.get("phase_type")}')
print(f'stage4_pass: {s3.get("stage4_pass")}')
print(f'validation:')
print(json.dumps(s3.get('validation', {}), indent=2, ensure_ascii=False, default=str))
print(f'stage4:')
print(json.dumps(s3.get('stage4', {}), indent=2, ensure_ascii=False, default=str))

print()
print('DONE')
