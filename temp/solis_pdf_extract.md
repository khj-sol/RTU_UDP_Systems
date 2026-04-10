# Solis 50kW 3-Phase Inverter — Modbus Register Reference

Source: Solis-PV_799467197-RS485-MODBUS-Protocol-V19.pdf
Target: Solis 25-50K / 50-70K series, 4 MPPT, 8 strings (2 strings per MPPT)

## Conventions

- Slave address: 1-247, default device address per inverter (no duplicates on bus)
- Baud: 9600, 8N1, RTU mode
- Word order (U32/S32): **HL** (high word first, then low word; high byte first within word)
- Function codes:
  - FC04: read input registers (3000-39999) — operation/measurement data (5.2)
  - FC03: read holding registers (4000-49999) — settings (5.3)
  - FC06/FC10: write single/multiple holding registers (5.3)
- IMPORTANT (per PDF section 5.2/5.3): "register address needs to offset one bit". The decimal addresses below are the **document addresses**; on the wire send `address - 1` (register zero-based offset). Hex column shows the document address as hex.
- For 50K (3-phase, 4 MPPT, ≥4 PV inputs): use MPPT V/I from 3500-3546 and string V/I from 3287-3338. Not 3022-3029 (those apply only when DC input count <4).

---

## 1. Device Info (FC04)

| Address (hex) | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 3000 (0xBB8) | Product model code | 04 | U16 | 1 | - | Inverter model id |
| 3001 (0xBB9) | DSP software version | 04 | U16 | 1 | - | High byte = slave DSP, low byte = main DSP |
| 3002 (0xBBA) | HMI major version | 04 | U16 | 1 | - | Low byte only |
| 3003 (0xBBB) | AC output type | 04 | U16 | 1 | - | 0=1ph, 1=3ph 4-wire, 2/3=3ph 3-wire |
| 3004 (0xBBC) | DC input type | 04 | U16 | 1 | - | n = number of DC inputs (0=1, 3=4...) |
| 3061-3064 (0xBF5-0xBF8) | Inverter SN_1..SN_4 | 04 | U16 x4 | - | - | Hex display, see PDF p8 |
| 3230-3247 (0xC9E-0xCAF) | SN Number (ASCII) | 04 | STRING(36) | - | - | Each reg = 2 ASCII chars |
| 3108 (0xC24) | Master DSP sub version | 04 | U16 | 1 | - | Combine with 3001 |

---

## 2. AC Measurements (FC04)

| Address (hex) | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 3034 (0xBDA) | Phase A voltage (or AB line) | 04 | U16 | 0.1 | V | Phase voltage when 3003=1 |
| 3035 (0xBDB) | Phase B voltage (or BC line) | 04 | U16 | 0.1 | V | |
| 3036 (0xBDC) | Phase C voltage (or CA line) | 04 | U16 | 0.1 | V | |
| 3037 (0xBDD) | Phase A current | 04 | U16 | 0.1 | A | |
| 3038 (0xBDE) | Phase B current | 04 | U16 | 0.1 | A | |
| 3039 (0xBDF) | Phase C current | 04 | U16 | 0.1 | A | |
| 3043 (0xBE3) | Grid frequency | 04 | U16 | 0.01 | Hz | |
| 3005-3006 (0xBBD) | Active power (total) | 04 | S32 HL | 1 | W | |
| 3056-3057 (0xBF0) | Reactive power (total) | 04 | S32 HL | 1 | Var | |
| 3058-3059 (0xBF2) | Apparent power | 04 | S32 HL | 1 | VA | |
| 3060 (0xBF4) | Real-time power factor | 04 | S16 | 0.001 | - | 50-70K/255K only |
| 3052 (0xBEC) | Actual power factor adjustment | 04 | S16 | 0.001 | - | Fallback PF readback |
| 3009-3010 (0xBC1) | Total energy (lifetime) | 04 | U32 HL | 1 | kWh | |
| 3015 (0xBC7) | Energy today | 04 | U16 | 0.1 | kWh | |
| 3011-3012 (0xBC3) | Energy this month | 04 | U32 HL | 1 | kWh | |
| 3013-3014 (0xBC5) | Energy last month | 04 | U32 HL | 1 | kWh | |
| 3017-3018 (0xBC9) | Energy this year | 04 | U32 HL | 1 | kWh | |
| 3109 (0xC25) | Real-time power percentage | 04 | U16 | 0.01 | % | |
| 3007-3008 (0xBBF) | Total DC output power | 04 | U32 HL | 1 | W | |

---

## 3. Inverter Status (FC04)

| Address (hex) | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 3044 (0xBE4) | Inverter status | 04 | U16 | 1 | - | See Appendix 1 (use with 3030 alarm) |
| 3030 (0xBD6) | Alarm code data | 04 | U16 | 1 | - | Sub-code paired with 3044 |
| 3041 (0xBE1) | Standard working mode | 04 | U16 | 1 | - | 00=NoResp, 01=Volt-Watt, 02=Volt-Var, 03=Fixed PF, 04=Fixed Reactive, 05=Power-PF |
| 3049 (0xBE9) | Inverter control word | 04 | U16 | 1 | - | See Appendix 3 (protection enables) |
| 3096-3100 (0xC18-0xC1C) | Fault Code 01..05 | 04 | U16 x5 | 1 | - | |
| 3050 (0xBEA) | Actual limited active power | 04 | U16 | 1 | % | 10000=100% |

### Status Code Quick Reference (Appendix 1, 4G+ series, paired with code 3044)

| Code | Display |
|---|---|
| 0x0000 (0000) | Waiting / Normal |
| 0x0000 (0001) | Grid Off |
| 0x0001 | OpenRun |
| 0x0002 | SoftRun (Waiting) |
| 0x0003 (0000) | Generating |
| 0x0003 (0001) | LimByTemp (Over-Temp Derating) |
| 0x0003 (0002) | LimByFreq (Over-Freq Derating) |
| 0x0003 (0004) | LimByVg (Over-Volt Derating) |
| 0x1xxx | Faults (e.g. 1057H Over-Load, 1058H DspSelfChk, 1059H Vg-Sample) |
| 0x2xxx | Comm/BMS faults (2014H DSP_Comm_FAIL, 2018H DRM_LINK_FAIL) |

(Full table p67-72 of PDF; pair status reg 3044 with sub-reg 3030 for sub-display.)

---

## 4. Per-MPPT (4 MPPTs) — for 50K (3004 ≥ 8)

Use MPPT block at 3500-3546. (Note: PDF labels per-MPPT power is NOT directly provided — compute power = V x I per MPPT.)

| Address (hex) | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 3500 (0xDAC) | MPPT 1 voltage | 04 | U16 | 0.1 | V | |
| 3501 (0xDAD) | MPPT 2 voltage | 04 | U16 | 0.1 | V | |
| 3502 (0xDAE) | MPPT 3 voltage | 04 | U16 | 0.1 | V | |
| 3503 (0xDAF) | MPPT 4 voltage | 04 | U16 | 0.1 | V | |
| 3530 (0xDCA) | MPPT 1 current | 04 | S16 | 0.1 | A | |
| 3531 (0xDCB) | MPPT 2 current | 04 | S16 | 0.1 | A | |
| 3532 (0xDCC) | MPPT 3 current | 04 | S16 | 0.1 | A | |
| 3533 (0xDCD) | MPPT 4 current | 04 | S16 | 0.1 | A | |

MPPT power: not a register — derive `Pn = Vn * In`. Total DC at 3007-3008 (U32, W).

(For DC input count <4, fall back to compact block 3022-3029.)

---

## 5. Per-String Currents (8 strings, 2 per MPPT)

For 50K (3004 ≥ 4) the PDF exposes individual PV string currents at 3301-3320 (PV1-PV20). 8-string mapping (2 strings per MPPT):

| Address (hex) | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 3301 (0xCE5) | PV1 current (MPPT1-A) | 04 | S16 | 0.1 | A | |
| 3302 (0xCE6) | PV2 current (MPPT1-B) | 04 | S16 | 0.1 | A | |
| 3303 (0xCE7) | PV3 current (MPPT2-A) | 04 | S16 | 0.1 | A | |
| 3304 (0xCE8) | PV4 current (MPPT2-B) | 04 | S16 | 0.1 | A | |
| 3305 (0xCE9) | PV5 current (MPPT3-A) | 04 | S16 | 0.1 | A | |
| 3306 (0xCEA) | PV6 current (MPPT3-B) | 04 | S16 | 0.1 | A | |
| 3307 (0xCEB) | PV7 current (MPPT4-A) | 04 | S16 | 0.1 | A | |
| 3308 (0xCEC) | PV8 current (MPPT4-B) | 04 | S16 | 0.1 | A | |
| 3287 (0xCD7) | PV string V/I combination flag | 04 | U16 | 1 | - | 0=1V→2I (8-string maps to 4 string voltages PVStr1..4) |
| 3321 (0xCF9) | PVStr1 voltage (= MPPT1) | 04 | U16 | 0.1 | V | |
| 3322 (0xCFA) | PVStr2 voltage (= MPPT2) | 04 | U16 | 0.1 | V | |
| 3323 (0xCFB) | PVStr3 voltage (= MPPT3) | 04 | U16 | 0.1 | V | |
| 3324 (0xCFC) | PVStr4 voltage (= MPPT4) | 04 | U16 | 0.1 | V | |
| 3299 (0xCE3) | Total PV voltage | 04 | U16 | 0.1 | V | |
| 3300 (0xCE4) | Total PV current | 04 | S16 | 0.1 | A | |

Notes: With 3287=0 ("1V→2I"), each PVStr voltage maps to 2 sequential PV currents — i.e. PVStr1 ↔ PV1+PV2, PVStr2 ↔ PV3+PV4, etc. This matches the 4 MPPT × 2 strings = 8 strings layout.

---

## 6. Temperature (FC04)

| Address (hex) | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 3042 (0xBE2) | Inverter temperature (AC NTC / IGBT) | 04 | S16 | 0.1 | °C | Primary heatsink temp |
| 3093 (0xC15) | Inverter temperature (secondary) | 04 | S16 | 0.1 | °C | |
| 3573 (0xDF5) | DC NTC1 temperature | 04 | U16 | 0.1 | °C | 250K series mainly |
| 3574-3577 | DC NTC2..NTC5 | 04 | U16 | 0.1 | °C | |

---

## 7. Control Registers (FC03 read / FC06 / FC10 write)

All addresses below are in the holding-register space (5.3). Wire address = doc address − 1.

| Address (hex) | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 3007 (0xBBF) | ON/OFF (grid-tie remote control) | 06 | U16 | 1 | - | 0xBE=ON, 0xDE=OFF, 0x10=Night ON enable, 0x11=Night ON disable |
| 3052 (0xBEC) | Active power limit (local) | 06 | U16 | 0.01 | % | 10000=100%, range 0-110%; requires 3070=0xAA |
| 3070 (0xBFE) | Power limit switch enable | 06 | U16 | 1 | - | 0xAA=enable, 0x55=disable |
| 3031 (0xBD7) | Remote active power limit % | 06 | U16 | 0.01 | % | Effective when 3080-BIT2 remote enable=1 |
| 3051 (0xBEB) | Reactive power limitation | 06 | S16 | 0.01 | % | range -6000..+6000; needs 3071=0xA1; mode 04 |
| 3071 (0xBFF) | Reactive power switch | 06 | U16 | 1 | - | 0x55=off, 0xA1=Q% mode, 0xA2=PF mode |
| 3053 (0xBED) | PF setting | 06 | S16 | 0.001 | - | ±800..±1000, range -1..-0.8 / 0.8..1 |
| 3054 (0xBEE) | PF setting 02 (mode 03) | 06 | S16 | 0.001 | - | Needs 3071=0xA2; switches inverter to mode 03 |
| 3030 (0xBD6) | Night SVG Q set | 06 | S16 | 0.01 | % | range -60..+60% |
| 3022 (0xBCE) | Restart inverter | 06 | U16 | 1 | - | 0x55AA = restart (80-110K PRO only) |
| 3028 (0xBD4) | Restart HMI | 06 | U16 | 1 | - | 0xAA55 valid; needs 3 sends in 6s |
| 3027 (0xBD3) | DRM ON/OFF | 06 | U16 | 1 | - | 0=off, 0xAA=on |
| 3006 (0xBBE) | Slave address | 06 | U16 | 1 | - | Set new RS485 address |

### Control sequence reminders

- Power limit: write `3070=0xAA` first (enable) → write `3052=` percentage×100 (e.g. 8000 = 80%).
- Reactive Q%: write `3071=0xA1` → write `3051=` value×100.
- Fixed PF: write `3071=0xA2` → write `3054=` PF×1000 (signed). Inverter switches to mode 03.
- ON/OFF: write `3007=0xBE` (ON) or `0xDE` (OFF).
- Power-off saving for 3051/3052/3053/3054 requires register 3069 (see PDF 5.3).

---

## Notes / Caveats

- Solis "address − 1" convention: Most Solis tools and the protocol explicitly state subtract 1 from the document register address before placing on the wire (e.g. document 3000 → frame address 2999 = 0x0BB7).
- 50K 3-phase 4 MPPT model falls under the "25-50K/50-70K/80-110K" group (model code 1121H at register 35000).
- For 50K (4 DC inputs ≥ 4) the per-MPPT block to use is **3500-3546**, and per-string currents come from **3289-3320** (active 8 strings = 3301-3308).
- Total active power (3005-3006) is S32 in **W** — divide by 1000 for kW.
- Status decoding: read **both** 3044 (main status) and 3030 (sub-code); concatenate per Appendix 1 to get final state string.
- Per-MPPT power register does NOT exist — compute as V×I.
- Heatsink/IGBT temperature register: 3042 is the main one; 3093 is a secondary sensor. NTC blocks 3570-3577 are populated only on 250K-class units.
