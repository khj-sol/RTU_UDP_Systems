# Stage1 Benchmark v4.2

`benchmark_stage1.py`는 v4.2 추출 구조를 비교합니다.

## 기본 비교군

```bash
python model_maker_web_v4/benchmark_stage1.py
```

기본 모드:

- `rule_only`
- `layout_first`
- `rapidocr_only`

Legacy 모드는 기본 실행하지 않습니다.

```bash
python model_maker_web_v4/benchmark_stage1.py --include-legacy --timeout-sec 600
```

Legacy 대상:

- `nemotron_ocr_en`
- `nemotron_ocr_multi`
- `full`
- `phi_only`

## 이미지 fixture

```bash
python model_maker_web_v4/benchmark_stage1.py \
  --make-image-fixtures \
  --fixture-mode both \
  --modes rule_only layout_first rapidocr_only
```

`fixture-mode=image`에서는 `rapidocr_only`만 실행합니다.

## 결과 컬럼

기존 H01/DER/x_fields 컬럼에 더해 아래 extractor 통계를 기록합니다.

| Column | 의미 |
|--------|------|
| `layout_used` | layout extractor 사용 여부 |
| `layout_blocks` | PyMuPDF text block 수 |
| `layout_tables` | 감지된 table 수 |
| `rapidocr_used` | RapidOCR fallback/only 사용 여부 |
| `rapidocr_pages` | OCR 처리 페이지 수 |
| `ocr_boxes` | OCR text box 수 |
| `valid_register_rows` | register candidate row 수 |
| `extractor_fallback_reason` | fallback/실패 이유 |

## 판정 원칙

- Model/SN은 프로토콜 맵에 없으므로 PASS/FAIL과 `x_fields`에서 제외합니다.
- v4.2의 1차 목표는 “큰 VLM 없이 빠르게 Stage1 완료”입니다.
- 매칭 정확도 개선은 extractor 후보를 기존 rules/synonym/reference matching에 안정적으로 전달하는 방식으로 진행합니다.
