# Model Maker Web v4

Modbus PDF에서 RTU 호환 `*_registers.py`를 생성하는 웹 도구입니다.

## v4.2 기본 방향

v4.2의 기본 Stage1 추출 모드는 `layout_first`입니다.

- 디지털 PDF: PyMuPDF/PyMuPDF Layout 계열 구조 추출을 우선 사용합니다.
- 스캔/이미지 페이지: 텍스트/표 후보가 부족한 페이지에만 RapidOCR ONNX를 fallback으로 사용합니다.
- Nemotron OCR/Nemotron-Nano-VL-8B는 legacy/debug 전용이며 기본 경로에서 제외되었습니다.
- 최종 매칭 판단은 계속 `rules + synonym_db + reference map + definitions`가 담당합니다.
- Model/SN은 프로토콜 맵 대상이 아니므로 검증 실패/x_fields/semantic 매칭 대상에서 제외합니다.

## 실행

```bash
# inverter_model_maker/ 디렉터리에서
START_모델메이커_WEB_v4.bat
```

브라우저: http://localhost:8083

## 주요 설정

`mm_settings.json`:

```json
{
  "layout": {
    "enabled": true,
    "min_valid_rows": 5,
    "image_dpi": 200
  },
  "rapidocr": {
    "enabled": true,
    "det_model_path": "",
    "rec_model_path": "",
    "rec_keys_path": "",
    "lang": "english",
    "device": "cpu"
  },
  "legacy_nemotron": {
    "enabled": false,
    "model_path": "C:/models/Nemotron-Nano-VL-8B",
    "device": "auto",
    "image_dpi": 200,
    "page_timeout": 120
  }
}
```

## Stage1 모드

| Mode | 용도 |
|------|------|
| `rule_only` | 기존 rule/table parser만 사용 |
| `layout_first` | 기본값. PyMuPDF 구조 추출 후 부족한 페이지만 RapidOCR fallback |
| `rapidocr_only` | 스캔 PDF/이미지 fixture 검증용 |
| `nemotron_ocr_en`, `nemotron_ocr_multi` | legacy/debug. 8B VLM이라 느릴 수 있음 |
| `full`, `phi_only` | legacy AI 경로 |

## 처리 파이프라인

```text
PDF
 -> existing rule/table parser
 -> PyMuPDF layout/table/text extraction
 -> sparse/scanned page only: render PNG -> RapidOCR ONNX
 -> register candidates
 -> rules/synonym/reference matching
 -> Stage1 Excel
 -> Stage2/Stage3 unchanged
```

## 설치 메모

기본 디지털 PDF 경로는 PyMuPDF만으로 동작합니다. RapidOCR fallback을 사용하려면 `backend/requirements.txt`의 `rapidocr-onnxruntime`, `onnxruntime` 의존성이 필요합니다.

상용 배포 전에는 PyMuPDF Layout 계열 라이선스를 별도로 확인해야 합니다.
