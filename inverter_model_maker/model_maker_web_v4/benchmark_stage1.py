"""
Stage1 벤치마크 스크립트 — v4.1

모드 분류:
  기본(DEFAULT)  : rule_only          — AI 없이 규칙 기반만 (빠름)
  후보(CANDIDATE): nemotron_ocr_en    — 미구현, 즉시 mode_not_implemented
                   nemotron_ocr_multi — 미구현, 즉시 mode_not_implemented
  레거시(LEGACY) : full, phi_only     — --include-legacy 없이는 실행 안 됨
                                         결과에 legacy_too_slow_candidate 기록

사용 예시:
  # 기본: results/*/*.pdf × rule_only
  python benchmark_stage1.py

  # 후보 모드 포함 (미구현 → mode_not_implemented 즉시 기록)
  python benchmark_stage1.py --modes rule_only nemotron_ocr_en

  # legacy 포함 (timeout 기록 목적)
  python benchmark_stage1.py --include-legacy --timeout-sec 30

  # image fixture 생성 (향후 OCR 검증용)
  python benchmark_stage1.py --make-image-fixtures --fixture-mode both

  # 특정 PDF
  python benchmark_stage1.py --pdf model_maker_web_v4/results/EG4/*.pdf

결과 파일:
  benchmark_results/stage1_benchmark_YYYYMMDD_HHMMSS.json / .csv

지표:
  pdf          - PDF 파일명
  mode         - 실행 모드
  elapsed_sec  - 처리 시간(초); timeout 시 실제 경과 시간
  timeout      - 타임아웃 발생 여부 (bool)
  notes        - legacy_too_slow_candidate 등 메모
  h01_matched  - H01 매칭 성공 수
  h01_total    - H01 전체 수
  der_matched  - DER 매칭 성공 수
  der_total    - DER 전체 수
  max_mppt     - 최대 MPPT 채널 수
  max_string   - 최대 String 채널 수
  review_count - REVIEW 항목 수 (수동 확인 필요)
  x_fields     - H01 매칭 실패(X) 필드 목록 (Model/SN 제외)
  info_model   - 모델명 레지스터 매칭 여부 (참고용, PASS 판정 무관)
  info_sn      - 시리얼넘버 레지스터 매칭 여부 (참고용, PASS 판정 무관)
  ai_fallback  - AI 대신 rule-based 폴백 여부
  ai_errors    - AI 에러 메시지
  error        - 예외/상태 메시지 (정상이면 null)

주의:
  - Model/SN은 이 프로토콜 맵에 없는 항목 → PASS/FAIL 판정 및 x_fields에서 제외.
  - info_model / info_sn 은 참고 컬럼으로만 기록.
  - full / phi_only 는 legacy 모드 — 10분+ 소요 가능, --include-legacy 필수.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import shutil
import sys
import tempfile
import threading
import time
from datetime import datetime

# ── 패키지 루트(inverter_model_maker/)를 sys.path에 추가 ──────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))   # model_maker_web_v4/
_PKG_ROOT  = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))  # inverter_model_maker/
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)

# ── Model/SN 제외 목록 ────────────────────────────────────────────────────────
_INFO_FIELDS: frozenset[str] = frozenset({
    'model', 'sn', 'serial_number', 'model_name',
    'device_model', 'device_sn',
})

# ── 모드 분류 ──────────────────────────────────────────────────────────────────
DEFAULT_MODES:   tuple[str, ...] = ('rule_only', 'layout_first', 'rapidocr_only')
CANDIDATE_MODES: tuple[str, ...] = ('layout_first', 'rapidocr_only')
LEGACY_MODES:    tuple[str, ...] = ('nemotron_ocr_en', 'nemotron_ocr_multi', 'full', 'phi_only')
ALL_MODES:       tuple[str, ...] = DEFAULT_MODES + LEGACY_MODES

_NOT_IMPLEMENTED_MODES: frozenset[str] = frozenset()

_DEFAULT_TIMEOUT_SEC = 180


# ── 헬퍼 ──────────────────────────────────────────────────────────────────────

def _collect_x_fields(table: list[dict]) -> str:
    """H01 match table에서 status=='X' 필드명 수집. Model/SN 계열 제외."""
    fields = [
        r['field'] for r in table
        if r.get('status') == 'X' and r.get('field', '') not in _INFO_FIELDS
    ]
    return ', '.join(fields)


def _console_safe(value) -> str:
    return str(value).encode('cp949', errors='replace').decode('cp949')


def _make_progress(log: dict):
    """->->-> ->-> + AI fallback/error ->-> progress callback."""
    def cb(msg: str, level: str = 'info'):
        print(f"    [{level.upper():5s}] {_console_safe(msg)}", flush=True)
        if level == 'error' and '[AI]' in msg:
            log['ai_errors'].append(msg)
        if level == 'warn' and 'rule-based' in msg:
            log['ai_fallback'] = True
    return cb


def _build_ai_settings(mode: str) -> dict | None:
    """mode → ai_settings dict. rule_only → None (AI 사용 안 함)."""
    if mode in ('rule_only', 'none'):
        return None
    try:
        from model_maker_web_v4.backend.api_routes import _load_ai_settings
        settings = _load_ai_settings()
    except Exception as e:
        print(f"  [WARN ] ai_settings 로드 실패: {e} — 빈 설정으로 진행", flush=True)
        settings = {}

    settings['ai_mode'] = mode
    if mode == 'phi_only':
        settings['qwen_enabled'] = False
    return settings


def _empty_row(pdf_name: str, mode: str, input_type: str = 'original') -> dict:
    return {
        'pdf':               pdf_name,
        'mode':              mode,
        'input_type':        input_type,
        'elapsed_sec':       None,
        'timeout':           False,
        'notes':             'legacy_too_slow_candidate' if mode in LEGACY_MODES else '',
        'h01_matched':       None,
        'h01_total':         None,
        'der_matched':       None,
        'der_total':         None,
        'max_mppt':          None,
        'max_string':        None,
        'review_count':      None,
        'x_fields':          None,
        'info_model':        None,
        'info_sn':           None,
        'ai_fallback':       None,
        'ai_errors':         None,
        'ocr_regs_extracted': None,
        'layout_used':       None,
        'layout_blocks':     None,
        'layout_tables':     None,
        'rapidocr_used':     None,
        'rapidocr_pages':    None,
        'ocr_boxes':         None,
        'valid_register_rows': None,
        'extractor_fallback_reason': None,
        'error':             None,
    }


# ── 실행 핵심 ─────────────────────────────────────────────────────────────────

def _run_one_inner(pdf_path: str, mode: str, temp_base: str) -> dict:
    """Stage1 실제 호출. thread 안에서 실행된다."""
    from model_maker_web_v4.backend.pipeline.stage1 import (
        run_stage1, NotRegisterMapError,
    )

    pdf_name = os.path.basename(pdf_path)
    tmp_dir  = tempfile.mkdtemp(dir=temp_base)
    ai_log: dict = {'ai_fallback': False, 'ai_errors': []}
    row = _empty_row(pdf_name, mode, input_type='original')

    try:
        ai_settings = _build_ai_settings(mode)
        progress    = _make_progress(ai_log)

        t0     = time.perf_counter()
        result = run_stage1(
            input_path=pdf_path,
            output_dir=tmp_dir,
            device_type='inverter',
            progress=progress,
            ai_settings=ai_settings,
        )
        elapsed = time.perf_counter() - t0

        h01        = result.get('h01_match', {})
        der        = result.get('der_match', {})
        meta       = result.get('meta', {})
        info_match = result.get('info_match', {})
        extractor_stats = result.get('extractor_stats', {}) or {}

        row.update({
            'elapsed_sec':  round(elapsed, 2),
            'h01_matched':  h01.get('matched'),
            'h01_total':    h01.get('total'),
            'der_matched':  der.get('matched'),
            'der_total':    der.get('total'),
            'max_mppt':     meta.get('max_mppt'),
            'max_string':   meta.get('max_string'),
            'review_count': result.get('review_count'),
            'x_fields':     _collect_x_fields(h01.get('table', [])),
            'info_model':   info_match.get('model'),   # 참고용
            'info_sn':      info_match.get('sn'),      # 참고용
            'ai_fallback':  ai_log['ai_fallback'],
            'ai_errors':    '; '.join(ai_log['ai_errors']) if ai_log['ai_errors'] else '',
            'layout_used':   extractor_stats.get('layout_used'),
            'layout_blocks': extractor_stats.get('layout_blocks'),
            'layout_tables': extractor_stats.get('layout_tables'),
            'rapidocr_used': extractor_stats.get('rapidocr_used'),
            'rapidocr_pages': extractor_stats.get('rapidocr_pages'),
            'ocr_boxes': extractor_stats.get('ocr_boxes'),
            'valid_register_rows': extractor_stats.get('valid_register_rows'),
            'extractor_fallback_reason': extractor_stats.get('extractor_fallback_reason'),
        })

    except NotRegisterMapError as e:
        row['error'] = f'NotRegisterMapError: {e}'
    except Exception as e:
        row['error'] = f'{type(e).__name__}: {e}'
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return row


def _run_one(pdf_path: str, mode: str, temp_base: str, timeout_sec: int) -> dict:
    """PDF 1개 × mode 1개 실행. 미구현 모드 즉시 종료, timeout 처리 포함."""
    pdf_name = os.path.basename(pdf_path)

    # 미구현 모드 → 즉시 종료
    if mode in _NOT_IMPLEMENTED_MODES:
        row = _empty_row(pdf_name, mode)
        row['elapsed_sec'] = 0.0
        row['error']       = 'mode_not_implemented'
        return row

    result_holder: list = [None]
    exc_holder:    list = [None]

    def worker():
        try:
            result_holder[0] = _run_one_inner(pdf_path, mode, temp_base)
        except Exception as e:
            exc_holder[0] = e

    t  = threading.Thread(target=worker, daemon=True)
    t0 = time.perf_counter()
    t.start()
    t.join(timeout_sec)
    elapsed = time.perf_counter() - t0

    if t.is_alive():
        row = _empty_row(pdf_name, mode)
        row['timeout']     = True
        row['elapsed_sec'] = round(elapsed, 2)
        row['error']       = 'timeout'
        print(f"    [WARN ] TIMEOUT ({elapsed:.0f}s) -- thread은 background에서 계속 실행됩니다", flush=True)
        return row

    if exc_holder[0] is not None:
        row = _empty_row(pdf_name, mode)
        row['elapsed_sec'] = round(elapsed, 2)
        row['error']       = f'{type(exc_holder[0]).__name__}: {exc_holder[0]}'
        return row

    return result_holder[0]


# ── image fixture OCR 실행 ───────────────────────────────────────────────────

def _run_rapidocr_fixtures_inner(pdf_path: str, mode: str, fixtures_dir: str) -> dict:
    """Run RapidOCR directly on PNG/JPG fixtures. H01/DER scoring is not available here."""
    from model_maker_web_v4.backend.pipeline.rapidocr_adapter import extract_fixture_candidates

    pdf_name = os.path.basename(pdf_path)
    pdf_stem = os.path.splitext(pdf_name)[0]
    fix_dir = os.path.join(fixtures_dir, pdf_stem)
    row = _empty_row(pdf_name, mode, input_type='image_fixture')

    if not os.path.isdir(fix_dir):
        row['elapsed_sec'] = 0.0
        row['error'] = f'no_fixtures: {fix_dir}'
        return row

    ai_settings = _build_ai_settings(mode) or {}
    def log(msg, level='info'):
        print(f"    [{level.upper():5s}] {msg}", flush=True)

    try:
        t0 = time.perf_counter()
        result = extract_fixture_candidates(fix_dir, settings=ai_settings, log=log)
        elapsed = time.perf_counter() - t0
        row['elapsed_sec'] = round(elapsed, 2)
        row['ocr_regs_extracted'] = len(result.candidates)
        row['rapidocr_used'] = result.stats.get('rapidocr_used')
        row['rapidocr_pages'] = result.stats.get('rapidocr_pages')
        row['ocr_boxes'] = result.stats.get('ocr_boxes')
        row['valid_register_rows'] = result.stats.get('valid_register_rows')
        row['extractor_fallback_reason'] = result.stats.get('extractor_fallback_reason')
    except Exception as e:
        row['error'] = f'{type(e).__name__}: {e}'

    return row


def _run_rapidocr_fixtures(pdf_path: str, mode: str, fixtures_dir: str, timeout_sec: int) -> dict:
    """Run fixture OCR with timeout guard."""
    pdf_name = os.path.basename(pdf_path)

    if mode in _NOT_IMPLEMENTED_MODES:
        row = _empty_row(pdf_name, mode, 'image_fixture')
        row['elapsed_sec'] = 0.0
        row['error'] = 'mode_not_implemented'
        return row

    result_holder: list = [None]

    def worker():
        result_holder[0] = _run_rapidocr_fixtures_inner(pdf_path, mode, fixtures_dir)

    t = threading.Thread(target=worker, daemon=True)
    t0 = time.perf_counter()
    t.start()
    t.join(timeout_sec)
    elapsed = time.perf_counter() - t0

    if t.is_alive():
        row = _empty_row(pdf_name, mode, 'image_fixture')
        row['timeout'] = True
        row['elapsed_sec'] = round(elapsed, 2)
        row['error'] = 'timeout'
        print(f"    [WARN ] TIMEOUT ({elapsed:.0f}s)", flush=True)
        return row

    return result_holder[0]

# ── image fixture PNG 생성 ────────────────────────────────────────────────────

def _make_image_fixtures(pdf_path: str, fixtures_dir: str) -> int:
    """PDF 페이지를 PNG로 렌더링하여 저장. PyMuPDF(fitz) 필요. 저장된 페이지 수 반환."""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        print("  [WARN ] PyMuPDF(fitz) 미설치 → image fixture 생성 불가. pip install pymupdf", flush=True)
        return 0

    pdf_stem = os.path.splitext(os.path.basename(pdf_path))[0]
    out_dir  = os.path.join(fixtures_dir, pdf_stem)
    os.makedirs(out_dir, exist_ok=True)

    doc   = fitz.open(pdf_path)
    mat   = fitz.Matrix(2.0, 2.0)  # 2× → ~150 dpi
    count = 0
    for i, page in enumerate(doc):
        pix  = page.get_pixmap(matrix=mat)
        dest = os.path.join(out_dir, f'page_{i:03d}.png')
        pix.save(dest)
        count += 1
    doc.close()
    return count


# ── 결과 저장 ─────────────────────────────────────────────────────────────────

def _save_results(rows: list[dict], out_dir: str) -> tuple[str, str]:
    """JSON + CSV 저장. (json_path, csv_path) 반환."""
    os.makedirs(out_dir, exist_ok=True)
    ts   = datetime.now().strftime('%Y%m%d_%H%M%S')
    stem = f'stage1_benchmark_{ts}'

    json_path = os.path.join(out_dir, f'{stem}.json')
    csv_path  = os.path.join(out_dir, f'{stem}.csv')

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    if rows:
        fieldnames = list(rows[0].keys())
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return json_path, csv_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Stage1 벤치마크 — PDF × 모드 측정 (v4.1)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--modes', nargs='+',
        choices=ALL_MODES,
        default=list(DEFAULT_MODES),
        metavar='MODE',
        help=(
            f'측정할 모드 목록 (기본: {" ".join(DEFAULT_MODES)}) '
            f'| 전체: {", ".join(ALL_MODES)}'
        ),
    )
    parser.add_argument(
        '--include-legacy', action='store_true',
        help='full / phi_only legacy 모드 포함 (느림; legacy_too_slow_candidate 기록)',
    )
    parser.add_argument(
        '--timeout-sec', type=int, default=_DEFAULT_TIMEOUT_SEC, metavar='N',
        help=f'PDF 1개/모드 1개당 최대 실행 시간(초) (기본: {_DEFAULT_TIMEOUT_SEC})',
    )
    parser.add_argument(
        '--pdf', nargs='+', metavar='PATH',
        help='PDF 파일 경로 (기본: results/*/*.pdf 자동 수집)',
    )
    parser.add_argument(
        '--out', default=os.path.join(SCRIPT_DIR, 'benchmark_results'), metavar='DIR',
        help='결과 저장 디렉터리 (기본: benchmark_results/)',
    )
    parser.add_argument(
        '--make-image-fixtures', action='store_true',
        help='PDF → PNG 페이지 이미지 생성 (향후 OCR 어댑터 검증용; PyMuPDF 필요)',
    )
    parser.add_argument(
        '--fixture-mode', choices=('original', 'image', 'both'), default='original',
        metavar='{original,image,both}',
        help=(
            '벤치마크 입력 형태 (original=PDF, image=PNG 페이지, both=둘 다; 기본: original). '
            'image / both 는 자동으로 --make-image-fixtures 포함.'
        ),
    )
    args = parser.parse_args()

    # ── 모드 결정 ─────────────────────────────────────────────────────────────
    modes: list[str] = list(args.modes)

    if args.include_legacy:
        for m in LEGACY_MODES:
            if m not in modes:
                modes.append(m)
    else:
        bad = [m for m in modes if m in LEGACY_MODES]
        if bad:
            print(f'[WARN] legacy 모드 {bad} 는 --include-legacy 없이는 실행되지 않습니다.', flush=True)
            modes = [m for m in modes if m not in LEGACY_MODES]
            if not modes:
                print('[ERROR] 실행 가능한 모드가 없습니다.')
                sys.exit(1)

    # ── PDF 수집 ─────────────────────────────────────────────────────────────
    if args.pdf:
        pdf_paths: list[str] = []
        for p in args.pdf:
            expanded = glob.glob(p)
            pdf_paths.extend(expanded if expanded else [p])
    else:
        pdf_paths = sorted(glob.glob(os.path.join(SCRIPT_DIR, 'results', '*', '*.pdf')))

    if not pdf_paths:
        print('측정할 PDF가 없습니다. --pdf 또는 results/*/*.pdf 에 파일을 추가하세요.')
        sys.exit(1)

    temp_base    = os.path.join(SCRIPT_DIR, 'temp')
    fixtures_dir = os.path.join(SCRIPT_DIR, 'fixtures')
    os.makedirs(temp_base, exist_ok=True)

    # ── image fixture 생성 ───────────────────────────────────────────────────
    need_fixtures = args.make_image_fixtures or args.fixture_mode in ('image', 'both')
    if need_fixtures:
        print(f'\n=== Image Fixture 생성 ({len(pdf_paths)}개 PDF) ===')
        for pdf_path in pdf_paths:
            pdf_name = os.path.basename(pdf_path)
            n = _make_image_fixtures(pdf_path, fixtures_dir)
            print(f'  {pdf_name}: {n}페이지 → fixtures/{os.path.splitext(pdf_name)[0]}/')

    # fixture_mode 결정: image/both는 nemotron 모드만 fixture로 실행
    run_original = args.fixture_mode in ('original', 'both')
    run_fixture  = args.fixture_mode in ('image', 'both')
    fixture_modes = [m for m in modes if m == 'rapidocr_only']

    # ── 벤치마크 실행 ────────────────────────────────────────────────────────
    legacy_in_run = [m for m in modes if m in LEGACY_MODES]

    # 총 건수 계산
    orig_entries    = len(pdf_paths) * len(modes) if run_original else 0
    fixture_entries = len(pdf_paths) * len(fixture_modes) if run_fixture else 0
    total           = orig_entries + fixture_entries

    print(f'\n=== Stage1 Benchmark ===')
    print(f'PDFs         : {len(pdf_paths)}개')
    print(f'Modes        : {modes}')
    print(f'fixture_mode : {args.fixture_mode}')
    print(f'Timeout      : {args.timeout_sec}s / 건')
    if legacy_in_run:
        print(f'[LEGACY]     : {legacy_in_run} -> legacy_too_slow_candidate')
    print(f'Total        : {total}건\n')

    rows: list[dict] = []
    idx = 0

    # ── original PDF 실행 ────────────────────────────────────────────────────
    if run_original:
        for pdf_path in pdf_paths:
            for mode in modes:
                idx     += 1
                pdf_name = os.path.basename(pdf_path)
                print(f'[{idx}/{total}] {pdf_name} | mode={mode} | input=original')
                row = _run_one(pdf_path, mode, temp_base, args.timeout_sec)
                rows.append(row)

                if row.get('timeout'):
                    print(f'  → TIMEOUT ({row["elapsed_sec"]}s)')
                elif row['error']:
                    print(f'  → ERROR: {row["error"]}')
                else:
                    print(
                        f'  → H01 {row["h01_matched"]}/{row["h01_total"]} | '
                        f'DER {row["der_matched"]}/{row["der_total"]} | '
                        f'elapsed {row["elapsed_sec"]}s | '
                        f'x=[{row["x_fields"]}]'
                    )

    # ── image fixture OCR 실행 (nemotron 모드만) ─────────────────────────────
    if run_fixture and fixture_modes:
        print(f'\n--- Image Fixture OCR ({len(pdf_paths)} PDFs x {len(fixture_modes)} modes) ---')
        for pdf_path in pdf_paths:
            for mode in fixture_modes:
                idx     += 1
                pdf_name = os.path.basename(pdf_path)
                print(f'[{idx}/{total}] {pdf_name} | mode={mode} | input=image_fixture')
                row = _run_rapidocr_fixtures(pdf_path, mode, fixtures_dir, args.timeout_sec)
                rows.append(row)

                if row.get('timeout'):
                    print(f'  → TIMEOUT ({row["elapsed_sec"]}s)')
                elif row['error']:
                    print(f'  → ERROR: {row["error"]}')
                else:
                    print(
                        f'  → OCR regs={row["ocr_regs_extracted"]} | '
                        f'elapsed {row["elapsed_sec"]}s'
                    )

    json_path, csv_path = _save_results(rows, args.out)
    print(f'\n결과 저장:')
    print(f'  JSON: {json_path}')
    print(f'  CSV : {csv_path}')
    print(f'\n완료 ({idx}건)')


if __name__ == '__main__':
    main()


