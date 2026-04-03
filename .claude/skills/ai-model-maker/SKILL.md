---
name: ai-model-maker
description: AI로 새 인버터 Modbus PDF를 분석하여 Stage 1 매칭을 완성합니다. 모델메이커 자동 매칭이 실패하거나 X 필드가 있을 때 사용합니다.
argument-hint: "[PDF 파일 경로]"
disable-model-invocation: true
allowed-tools: Bash Read Write Edit Grep Glob
---

# AI Model Maker - 인버터 PDF 자동 매칭

새 인버터 Modbus PDF를 AI로 분석하여 H01 필드, MPPT/String, STATUS/ALARM 정의를 완벽히 매칭합니다.

## 사용법

```
/ai-model-maker <PDF 파일 경로>
```

## 실행 절차

### 1단계: Stage 1 자동 실행 + 진단

```python
from model_maker_web_v2.backend.pipeline.stage1 import run_stage1
result = run_stage1(pdf_path, output_dir)
```

H01 매칭 결과를 확인하고 X(미매칭) 필드를 식별합니다.

### 2단계: X 필드 AI 분석

X 필드가 있으면 PDF를 PyMuPDF로 직접 파싱하여:

#### pv_power 미매칭
- PDF 전체에서 DC power, PV power, input power, output power, pac 키워드 검색
- 발견 시: `assign_h01_field`에 키워드 추가 또는 HANDLER fallback

#### cumulative_energy 미매칭
- total energy, cumulative, lifetime energy, Eac total, Einv all, energy total, energy since 키워드 검색
- Growatt 형식 (Eac/Einv), EG4 형식 (Einv all), SunSpec 형식 (I_AC_Energy_WH) 등 확인
- SunSpec 텍스트 약어: `WH` (Total yield) → `defn_lower.strip() == 'wh'` 체크 in assign_h01_field

#### status 미매칭
- inverter mode, work mode, operating mode/state, running status, device status 검색
- 단독 "State", "Running" 등 짧은 이름도 확인
- nospace 패턴: workmode, invworkmode, sysstatemode, workingmodes, currentstatus
- SunSpec 텍스트 약어: `St` (Operating status) → SUNSPEC_STATUS_FIELDS에 `'st': 'STATUS'` 추가
  - **주의**: 짧은 패턴('st')은 서브스트링 오매칭 방지를 위해 완전 일치만 허용 (len(pattern) > 2 가드)

#### alarm 미매칭
- fault code, error code, alarm code, warning code, faultcode, warningcode 검색
- `_alarm_score` 제외 규칙 확인 (addr < 2000이면 키워드 필수)
- distribute_alarms 동작 확인
- SunSpec 텍스트 약어: `StVnd` (Manufacturer status) → SUNSPEC_ALARM_FIELDS에 `'stvnd': 'ALARM'` 추가
  - **ALARM 체크를 STATUS보다 먼저 실행** 필수 — StVnd는 'st' 포함으로 STATUS에 오매칭됨

#### MPPT/String 미매칭
- Vpv{N}, PV{N}Curr, Ppv{N}, PV{N}Watt, MPPT zone, DC{N}, Input {N} 패턴 검색
- SunSpec DCA/DCV/DCW 반복 블록 감지
- Central type (번호 없는 DC/PV/Input) → MPPT=1
- MPPT current 없으면: voltage + power → handler 계산

### 3단계: FC(Function Code) 구분 확인

PDF에 Input Register/Holding Register 섹션 구분이 있는지 확인:
- "input register", "input reg", "read only register"
- "holding register", "hold register", "holding reg", "parameter setting"
- 같은 주소가 FC에 따라 다른 레지스터 → FC 태깅

### 4단계: SunSpec 감지

C_SunSpec_ID, C_Manufacturer, I_Status 등 SunSpec 패턴이 3개 이상이면:
- `sunspec.py` 자동 적용
- I_STATUS_OFF~STANDBY → EXCLUDE (값 정의, 레지스터 아님)
- I_Status_Vendor → ALARM
- DCA/DCV/DCW 반복 블록 → MPPT별 매핑
- `solaredge_definitions.json` 에러코드 적용

#### SunSpec 텍스트 형식 (SMA 등 — 테이블 없는 PDF)

표준 테이블 파싱으로 0 레지스터 → `NotRegisterMapError` 발생 시:
- `_parse_sunspec_text_registers(pages)` 자동 폴백 실행
- 형식: `40200 Active power (W), in WW_SF (40201): sum of all inv...`
  - 10진수 5자리 주소 (40001~) → Modbus 주소 = reg_no - 1
  - 괄호 내 첫 번째 영문 약어 추출: `(W)` → `W`, `(DCA)` → `DCA`
  - 독립 주소 행 처리: `40225\nManufacturer-specific status code (StVnd)` → 자동 병합
- 주요 약어 → 분류:
  - `Mn`, `Md`, `SN`, `Vr` → INFO (SUNSPEC_INFO_FIELDS에 추가)
  - `St` → STATUS (exact match only)
  - `StVnd` → ALARM
  - `WH` → MONITORING → cumulative_energy
  - `DCA`, `DCV`, `DCW` → MONITORING → MPPT (기존 detect_sunspec_mppt 자동 처리)
- `is_sunspec_pdf()`: 제조사명 접두사 처리 (`SMA-PV` → `SMA` 추출 후 SUNSPEC_MANUFACTURERS 검색)
- 정의 파일: `definitions/sma_definitions.json` (StVnd 상태 코드 13개)

### 5단계: STATUS/ALARM 정의 추출

PDF에서 정의 테이블을 수동 검색:
- hex mode: `0x00 | Standby`, `0x01 | Fault`
- num mode: `0: waiting, 1: normal, 3: fault`
- NN---mode: `00---No response mode`
- bitfield: `Bit0 | Grid Volt Low`, `BIT00 | Backup overvoltage fault`
- Value|Description 테이블: ABB 형식
- Enum 인라인: `Enum • 0x0002 Reconnecting`
- Appendix fault codes: `002 | 0x0002 | Grid overvoltage | Fault`

발견 시 `definitions/{manufacturer}_definitions.json` 생성/업데이트

### 6단계: 모델 메이커 프로그램 수정

AI 분석으로 발견한 패턴이 기존 코드에 없으면:
1. `__init__.py` `detect_channel_number`: 새 MPPT/String 패턴 추가
2. `stage1.py` `assign_h01_field`: 새 키워드 추가
3. `rules.py` `classify_register_with_rules`: 새 분류 규칙 추가
4. `rules.py` `_alarm_score`: alarm 제외 규칙 수정
5. `stage1.py` `_scan_pdf_definitions`: 새 정의 테이블 패턴 추가
6. **SunSpec 텍스트 형식 새 약어**:
   - `sunspec.py` `SUNSPEC_STATUS_FIELDS`: 새 STATUS 약어 추가 (exact match 여부 확인)
   - `sunspec.py` `SUNSPEC_ALARM_FIELDS`: 새 ALARM 약어 추가
   - `sunspec.py` `classify_sunspec_register` INFO 텍스트 체크: 새 INFO 약어 추가
   - `stage1.py` `_parse_sunspec_text_registers`: 파서 예외 처리 추가
   - `definitions/{manufacturer}_definitions.json`: 상태 코드 추가

### 7단계: 검증

수정 후:
1. 새 인버터 H01 매칭 확인 (matched/total, X=[])
2. MPPT/String 수 확인
3. StDef/AlDef 확인
4. 기존 인버터 회귀 테스트 (6개 PROD + 11개 TEST)
5. 커밋 → push → main merge

### 8단계: 결과 보고

```
=== {인버터명} Stage 1 결과 ===
H01: NN/NN (100%)
INFO: N개 (Model=OK, SN=OK)
MPPT: N (V=N, I=N)
String: N (모니터링: Yes/No)
Status: {레지스터명} (StDef=N개)
Alarm: {레지스터명} (AlDef=N개)
IV Scan: Yes/No
FC: 03/04 구분 Yes/No
SunSpec: Yes/No
```

## 참고 파일

- `pipeline/__init__.py`: RegisterRow, detect_channel_number, 패턴 RE
- `pipeline/stage1.py`: run_stage1, assign_h01_field, scan_definition_tables
- `pipeline/rules.py`: classify_register_with_rules, _alarm_score, should_exclude
- `pipeline/sunspec.py`: SunSpec 표준 처리
- `pipeline/definitions/`: 제조사별 정의 JSON
- `pipeline/solaredge_definitions.json`: SunSpec 에러코드 349개
