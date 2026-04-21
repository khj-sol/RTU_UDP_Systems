# Model Maker Web v4

인버터 Modbus PDF → RTU 호환 `*_registers.py` 자동 생성 웹 도구.  
Nemotron OCR(로컬 HuggingFace 모델) 기반 AI 보조 추출 지원.

---

## 실행

```bash
# inverter_model_maker/ 디렉터리에서
START_모델메이커_WEB_v4.bat
```

브라우저: http://localhost:8083

---

## 설치

### 1. 패키지 설치

```bash
INSTALL_패키지.bat
```

`backend/requirements.txt` 기준으로 의존성 설치:

| 패키지 | 버전 |
|--------|------|
| fastapi | >=0.110.0 |
| uvicorn | >=0.29.0 |
| transformers | >=4.50.0 |
| torch | >=2.3.0 |
| huggingface-hub | >=0.24.0 |
| PyMuPDF | >=1.24.0 |
| Pillow | >=10.0.0 |
| accelerate | >=0.33.0 |

### 2. Nemotron 모델 다운로드 (AI 모드 사용 시)

```bash
INSTALL_Nemotron_Model.bat
```

- 모델: `nvidia/Llama-3.1-Nemotron-Nano-VL-8B-V1`
- 저장 위치: `C:\models\Nemotron-Nano-VL-8B`
- 용량: ~16GB, 30~60분 소요

---

## 설정

웹 UI 접속 후 설정 탭에서 변경하거나 `mm_settings.json`을 직접 편집:

```json
{
  "nemotron_ocr": {
    "model_path": "C:/models/Nemotron-Nano-VL-8B",
    "device": "auto",
    "image_dpi": 200
  }
}
```

| 항목 | 설명 |
|------|------|
| `model_path` | 로컬 모델 디렉터리 경로 |
| `device` | `"auto"` (GPU 자동) 또는 `"cpu"` |
| `image_dpi` | PDF → 이미지 변환 해상도 (기본 200) |

---

## 처리 파이프라인

```
PDF 업로드
  │
  ▼
Stage 1 — 레지스터 주소·이름 추출 (rule_only 또는 nemotron_ocr)
  │
  ▼
Stage 2 — H01/DER-AVM 필드 매핑
  │
  ▼
Stage 3 — *_registers.py 코드 생성 및 저장
```

### AI 모드

| 모드 | 설명 |
|------|------|
| `rule_only` | AI 없음 — 규칙 기반 regex 추출 (빠름) |
| `nemotron_ocr_en` | Nemotron 로컬 모델 — 영문 프롬프트 |
| `nemotron_ocr_multi` | Nemotron 로컬 모델 — 한/일 레이블 변환 포함 |

---

## 디렉터리 구조

```
model_maker_web_v4/
├── backend/
│   ├── main.py                  # FastAPI 앱 진입점 (포트 8083)
│   ├── api_routes.py            # REST API 라우터
│   ├── session_store.py         # 세션 관리
│   ├── ws_manager.py            # WebSocket 진행상황 푸시
│   ├── requirements.txt         # Python 의존성
│   └── pipeline/
│       ├── stage1.py            # PDF 파싱 + 레지스터 추출
│       ├── stage2.py            # H01/DER 필드 매핑
│       ├── stage3.py            # 코드 생성
│       ├── rules.py             # 규칙 기반 추출 엔진
│       ├── ai_nemotron_ocr.py   # Nemotron OCR 어댑터
│       └── definitions/         # 필드 정의 JSON
├── static/
│   └── index.html               # 웹 UI (단일 페이지)
├── mm_settings.json             # 모델 경로 등 로컬 설정
├── benchmark_stage1.py          # 정확도 벤치마크 스크립트
├── BENCHMARK_README.md          # 벤치마크 사용법
├── results/                     # 테스트 PDF 저장
├── benchmark_results/           # 벤치마크 결과 (자동 생성)
├── fixtures/                    # 이미지 fixture (자동 생성)
└── temp/                        # 세션 임시 파일 (자동 정리)
```

---

## 생성 파일 배포

Stage 3에서 저장한 `{protocol}_registers.py`를 RTU에 적용:

```bash
cp inverter_model_maker/common/{protocol}_registers.py common/
```

`config/rs485_ch*.ini`에 `protocol = {protocol}` 추가 → RTU 재시작 없이 동적 로딩.
