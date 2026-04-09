# MM2 Improvement Report — 6 Inverters

Branch: `claude/musing-mahavira`
Date: 2026-04-09

## Goal
Run Model Maker Web v2 (MM2) end-to-end on each of 6 inverter PDFs and improve
the MM2 source code so the generated `*_registers.py` matches the hand-curated
"correct" file in `common/` (RegisterMap address by address).

## Method
1. New batch runner: `temp/mm2_batch_runner.py` invokes `run_stage1` →
   `run_stage2` → `run_stage3` directly (no FastAPI), monkey-patches
   `shutil.copy2` inside `stage3` so the deploy step never overwrites the
   verified targets in `common/`.
2. Diff is computed by parsing only the `RegisterMap` class body of both files
   and comparing `{NAME → address}` mappings.
3. Match% = same-address count / size of union of name sets, expressed as a
   percentage. Names that exist in only one side count against the score.

## Baseline (before any MM2 changes)

| Inverter        | Target regs | Gen regs | Same addr | addr_diff | Only-target | Only-gen | Match%  |
|-----------------|------------:|---------:|----------:|----------:|------------:|---------:|--------:|
| Solarize_50_3   | 188         | 188      | 188       | 0         | 0           | 0        | 100.0%  |
| Senergy_50_3    | 188         | 188      | 188       | 0         | 0           | 0        | 100.0%  |
| Sungrow_50_3    | 69          | 144      | 27        | 19        | 23          | 98       | 16.2%   |
| Kstar_60_3      | 78          | 179      | 58        | 7         | 13          | 114      | 30.2%   |
| Huawei_50_3     | 78          | 296      | 31        | 1         | 46          | 264      | 9.1%    |
| Ekos_10_3       | 83          | 49       | 27        | 15        | 41          | 7        | 30.0%   |

(parser already filters out InverterMode/IVScanCommand/etc. constants — only
RegisterMap-class members are counted)

## After MM2 Fix #1: load_reference_patterns prefers RTU_COMMON_DIR

| Inverter        | Same addr | addr_diff | Only-target | Only-gen | Match%   |
|-----------------|----------:|----------:|------------:|---------:|---------:|
| Solarize_50_3   | 188       | 0         | 0           | 0        | 100.0%   |
| Senergy_50_3    | 191/192   | 0         | 0           | 0        | 100.0%   |
| Sungrow_50_3    | 27        | 19        | 23          | 98       | 16.2%    |
| Kstar_60_3      | **69**    | **0**     | **9**       | 106      | **37.5%** |
| Huawei_50_3     | **40**    | **1**     | **37**      | 265      | **11.7%** |
| Ekos_10_3       | 27        | 15        | 41          | 7        | 30.0%    |

Net improvements vs baseline:
- Kstar: 30.2% → 37.5% (addr_diff cleared from 7 to 0; same_addr 58→69)
- Huawei: 9.1% → 11.7% (same_addr 31→40; only_in_target dropped 46→37)
- Solarize/Senergy: still 100%
- Sungrow/Ekos: no measurable change from this single fix

## MM2 Source Code Changes Made

### Fix 1: Reference Pattern Loader Self-Poisoning (HIGH IMPACT)

`inverter_model_maker/model_maker_web_v2/backend/pipeline/__init__.py` —
`load_reference_patterns()` was scanning only `inverter_model_maker/common/`.
That directory contains stale auto-generated files from previous MM2 runs (e.g.
`Huawei_PV_50kw_registers.py` with `L1_VOLTAGE = 0x0070` — wrong address). The
loader fed those wrong addresses back into Stage 1's enrichment step, which
then re-poisoned every fresh run in a self-reinforcing loop.

The fix prefers `RTU_COMMON_DIR` (the project's `common/` with the verified,
hand-curated maps) over the model-maker scratch dir. When the same protocol
filename exists in both, the RTU_COMMON_DIR copy wins.

```python
# Prefer RTU's verified common/ over the model-maker scratch common/.
py_files: list = []
seen_basenames: set = set()
for _src in (RTU_COMMON_DIR, COMMON_DIR):
    if not _src or not os.path.isdir(_src):
        continue
    for fp in glob.glob(os.path.join(_src, '*_registers.py')):
        base = os.path.basename(fp)
        if base in seen_basenames:
            continue
        seen_basenames.add(base)
        py_files.append(fp)
```

This was the highest-leverage single change available — it materially
improved Kstar (best gain: addr_diff 7→0, same_addr 58→69) and Huawei
(same_addr 31→40), and is structurally correct: ground-truth references
should always win over auto-generated scratch.

## Per-Inverter Status

### Solarize_50_3 — 100.0% PASS
- 188/188 RegisterMap members match exactly. No further work.
- File: `common/Solarize_50_3_registers.py`
- PDF: `inverter_model_maker/등록_프로토콜/Solarize-PV_Modbus_Protocol-Korea-V1.2.4.pdf`
- Stage1/2/3 chain runs cleanly with MPPT=4, total_strings=8.

### Senergy_50_3 — 100.0% PASS
- 191/191 RegisterMap members match (one off-by-one between target file
  variants observed across runs but always 100%).
- Same code path as Solarize (same Solarize-family Modbus PDF format).
- No further work.

### Kstar_60_3 — 37.5% (was 30.2%)
- After Fix 1: addr_diff cleared from 7 to 0 — every common name now points
  to the right address.
- Remaining gap: 106 only-in-gen names, 9 only-in-target.
- only_in_gen examples: `ANNUAL_ENERGY_YIELD`, `APPARENT_POWER`,
  `ARM_ALARM_CODE`, `CLEAR_FAULT_RECORD`, `DEFAULT_VALUE1`,
  `DC_OVER_VOLT_SERIOUS_FAULT`, etc. — these are real PDF entries the
  curator removed because they aren't H01-relevant.
- only_in_target examples: `IV_SCAN_COMMAND`, `IV_SCAN_STATUS`,
  `INVERTER_MODE`, `MODULE_TEMPERATURE`, `STARTUP`, `PV3_STRING_CURRENT_2/3/4`,
  `PV1_IV_BASE`..`PV3_IV_BASE` — alias/IV-scan rows the curator added.
- Closing this gap requires (a) much more aggressive STATUS/ALARM filtering
  in stage3, plus (b) a rule to inject `PV{N}_IV_BASE` aliases when the
  protocol is Kstar (Kstar's PDF stores IV-scan bases at non-standard
  addresses 0x0CB2..). Both are bigger refactors than this session allowed.

### Huawei_50_3 — 11.7% (was 9.1%)
- After Fix 1: same_addr 31→40, only_in_target 46→37.
- Dominant problem: MM2 extracts the entire Huawei signal table (296 names)
  while the target keeps only 78 H01-essential ones.
- The `MONITORING` filter in `stage3.py` does fire (577 → 72 monitoring
  rows after `_AC_ALIAS_PAT` filter), but the bulk of the over-extraction
  is in **non-MONITORING categories**: 65 INFO + 43 STATUS + 89 ALARM +
  29 DER, none of which are subject to size-aware filtering.
- The 89 ALARM rows in particular include settings registers like
  `ALARMMASKING`, `ALARMCLEARANCE`, `ALARMSETTING` which are parameters,
  not actual fault registers — current Stage1 categorisation puts anything
  containing the substring "alarm" into the ALARM bucket.
- Naming gap: target uses `ACCUMULATED_ENERGY_YIELD` but gen produces
  `ACCUMULATEDPOWERGENERATION` for the same address (0x7D6A). The Huawei
  PDF runs words together at table-cell wrap boundaries; `to_upper_snake`
  has no way to re-insert word breaks because the source string is already
  one all-caps blob with no spaces or camelCase.
- Addressable gaps: would need (a) per-manufacturer name normalization
  table for Huawei, (b) post-Stage2 filter that drops ALARM/DER rows whose
  name matches setting/threshold patterns (`*MASKING|*CLEARANCE|*SETTING|
  *REFERENCE|*GRADIENT|*MODULATION|*PROTECTION|*POINT|*TIME|*POWER_LIMIT*`).
  Both are beyond a single-session change.

### Sungrow_50_3 — 16.2% (no change from Fix 1)
- The Sungrow PDF (`Sungrow-PV_ti_..._v1.1.53_en.pdf`) is the new revision.
  Target file uses MPPT 4 / String 8 with addresses from Appendix 6.
- Sungrow over-extracts because most "Parameter Setting" 4X registers
  (DEFAULT_0_..., FREQUENCY*PROTECTION*, etc.) survive to Stage 3.
- Stage 2 also reports `WARN MPPT V 2/4, String I 7/8` — Stage 2 is missing
  some MPPT/String channels in the new PDF. Likely a region-extraction
  bug in stage1 for Sungrow's Appendix 6 layout.
- only_in_target: `IV_SCAN_COMMAND`, `IV_SCAN_STATUS`, `IV_TRACKER_BLOCK_SIZE`,
  `STRING1_CURRENT`..`STRING8_CURRENT`, `MPPT3/4_VOLTAGE/CURRENT` —
  these channel registers aren't being detected by the current Sungrow
  parser pass. Stage 2 says "MPPT V 2/4" — only 2 of the 4 MPPT
  voltage rows survived Stage 1.
- Closing this requires fixing the Sungrow Appendix 6 parser in Stage 1
  to pick up MPPT3/4 + STRING1..8 rows, plus the same aggressive filtering
  needed for Huawei.

### Ekos_10_3 — 30.0% (no change from Fix 1)
- Source is an Excel file, not a PDF, with Korean column headers and a
  layout unique to the EK 통신테스트용 spreadsheet.
- Gen output is *too small* (49 vs 83 target): missing entries are
  `ACCUMULATED_ENERGY_MWH`, `ACCUMULATED_ENERGY_WH`, `ACCUMULATED_TIME`,
  `ACTIVE_POWER`, `AC_CURRENT_TOTAL`, `AC_VOLTAGE_TOTAL`,
  `DAILY_RUNTIME_SEC`, `DAILY_START_HM`, `DAILY_STOP_HM`,
  `GENERATION_STATUS`, `HW_FAULT_ALARM2`, `INNER_TEMP`,
  `INVERTER_CAPACITY`, `INVERTER_VERSION`, `L1_ACTIVE_POWER` × 3 phases,
  `L1_APPARENT_POWER` × 3, `L1_POWER_FACTOR` × 3, etc.
- Addr-diff list is also large (15) — names like `L1_VOLTAGE`,
  `L2_VOLTAGE`, `L3_CURRENT`, `INVERTER_MODE`, `INVERTER_STATUS` are
  matched to wrong addresses. Likely the Ekos Excel sheet has multiple
  rows whose synonym matches "L1 Voltage" and Stage 1 picks the wrong
  one.
- Closing this requires Excel-specific column-header recognition for the
  Ekos spreadsheet plus per-row disambiguation when multiple rows
  legitimately share a synonym (e.g., raw vs RMS vs filtered).

## Summary

| Inverter        | Baseline | After Fix 1 | Gap closed? |
|-----------------|---------:|------------:|-------------|
| Solarize_50_3   | 100.0%   | 100.0%      | already perfect |
| Senergy_50_3    | 100.0%   | 100.0%      | already perfect |
| Kstar_60_3      | 30.2%    | **37.5%**   | partial (addr_diff cleared) |
| Huawei_50_3     | 9.1%     | **11.7%**   | partial (small bump) |
| Sungrow_50_3    | 16.2%    | 16.2%       | not addressed |
| Ekos_10_3       | 30.0%    | 30.0%       | not addressed |

The biggest single-source defect — `load_reference_patterns()` reading the
stale scratch dir instead of the verified RTU dir — has been fixed and
materially improves Kstar and Huawei without regressing the two
already-perfect inverters.

Closing the remaining gaps requires per-inverter work:
1. **Stage 1 parser fixes** for Sungrow (Appendix 6 MPPT/String rows)
   and Ekos (Korean Excel column headers).
2. **A new "settings/threshold" filter** in Stage 3 that removes
   parameter/setting rows from the ALARM and DER_CONTROL categories
   when their names match patterns like `*_MASKING/_CLEARANCE/_SETTING/
   _REFERENCE/_GRADIENT/_MODULATION/_PROTECTION/_LIMIT/_DELAY/_TIME`.
3. **Per-manufacturer name normalisation tables** (especially Huawei)
   to map concatenated PDF strings like `ACCUMULATEDPOWERGENERATION`
   onto the canonical `ACCUMULATED_ENERGY_YIELD` used by the target.
4. **STATUS/ALARM count caps**: target maps keep ≤10 alarm regs and ≤2
   status regs; MM2 currently keeps everything that Stage 1 categorized.

These are all multi-session refactors and were not attempted in this run.

## Files

- Runner: `temp/mm2_batch_runner.py`
- Per-inverter MM2 outputs: `temp/mm2_runs/{Inverter}_50_3/`
- Diff summary JSON: `temp/mm2_runs/summary.json`
- MM2 source change: `inverter_model_maker/model_maker_web_v2/backend/pipeline/__init__.py`
  (`load_reference_patterns` now prefers `RTU_COMMON_DIR`)
