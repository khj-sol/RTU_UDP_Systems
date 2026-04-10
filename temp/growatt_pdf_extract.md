# Growatt 30kW 3-phase 3-MPPT PV Inverter — Modbus Register Extract

- **Source PDF**: `인버터프로토콜/인버터프로토콜/Growatt-PV_Modbus_RS485_RTU_V3-14.pdf`
- **Protocol Version**: V3.14 (2016-09-27)
- **Applies to**: Growatt PV grid-tie inverters (MAX/MID/TL3-X series, covers up to 3 MPPT + 8 strings)
- **Target model**: MID-25~40KTL3-X (30 kW, 3-phase, 3 MPPT, 6 strings)

## 1. Communication Setup

| Item | Value |
|------|-------|
| Framing | Modbus RTU |
| Byte format | 8N1 (8 data bits, no parity, 1 stop bit) |
| Baud rate | 9600 bps |
| Slave ID range | 1 – 247 (0 = broadcast) |
| Register size | 16-bit unsigned per register |
| Word order | **Big-endian (H then L)** — all 32-bit values use `H` at lower address, `L` at higher |
| Function codes | FC03 (Read Holding), FC04 (Read Input), FC06 (Write Single), FC16 (Write Multiple) |
| Max read/write length | 45 words per request; must align on multiples of 45 |
| Min CMD interval | 850 ms (1 s recommended) |
| Error response | `0x80 | FC` + Errornum byte + CRC |

Holding registers = configuration / control. Input registers = real-time measurement.

## 2. Device Info (Holding, FC03/FC06)

| Name | Addr (hex) | Type | Scale | Unit | FC | Notes |
|------|-----------|------|-------|------|----|----|
| FW version H / M / L | 0x0009 / 0x000A / 0x000B | U16 x3 | — | ASCII | 03 | Main firmware (3 chars) |
| FW version2 H / M / L | 0x000C / 0x000D / 0x000E | U16 x3 | — | ASCII | 03 | Control firmware |
| Serial No. 5..1 | 0x0017 – 0x001B | U16 x5 | — | ASCII | 03 | 5 registers, SN5 at low addr |
| Module H / L | 0x001C / 0x001D | U16 x2 | — | code | 03 | Inverter model code (&*5) |
| Com Address | 0x001E | U16 | — | — | 03/06 | Modbus slave ID |
| DTC (Device Type Code) | 0x002B | U16 | — | code | 03 | &*6 |
| TP (Tracker/Phase) | 0x002C | U16 | — | — | 03 | High byte = MPPT count, low byte = phase count. 30kW 3-phase 3-MPPT → `0x0303` |
| Manufacturer Info | 0x003C – 0x0043 | U16 x8 | — | ASCII | 03 | 8-register manufacturer string (moved from addr 13 in V2.06) |

## 3. Real-time Measurement (Input, FC04)

All addresses are Input Register addresses (FC04). Energy/Power uses 32-bit H:L pairs.

### 3.1 Status / Global

| Name | Addr | Type | Scale | Unit | Notes |
|------|------|------|-------|------|-------|
| Inverter Status | 0x0000 | U16 | — | enum | See section 4 |
| Ppv H / L | 0x0001 / 0x0002 | U32 | 0.1 | W | Total PV input power |

### 3.2 PV / MPPT (3 channels)

| Name | Addr | Type | Scale | Unit |
|------|------|------|-------|------|
| Vpv1 | 0x0003 | U16 | 0.1 | V |
| PV1Curr | 0x0004 | U16 | 0.1 | A |
| PV1Watt H / L | 0x0005 / 0x0006 | U32 | 0.1 | W |
| Vpv2 | 0x0007 | U16 | 0.1 | V |
| PV2Curr | 0x0008 | U16 | 0.1 | A |
| PV2Watt H / L | 0x0009 / 0x000A | U32 | 0.1 | W |
| PV3 Voltage | 0x0078 | U16 | 0.1 | V |
| PV3 Current | 0x0079 | U16 | 0.1 | A |
| PV3Watt H / L | 0x007A / 0x007B | U32 | 0.1 | W |

### 3.3 AC Output (3-phase)

| Name | Addr | Type | Scale | Unit | Notes |
|------|------|------|-------|------|-------|
| Pac H / L | 0x000B / 0x000C | U32 | 0.1 | W | Total active power |
| Fac | 0x000D | U16 | 0.01 | Hz | Grid frequency |
| Vac1 (R) | 0x000E | U16 | 0.1 | V | Phase R line-to-neutral |
| Iac1 (R) | 0x000F | U16 | 0.1 | A | |
| Pac1 H / L | 0x0010 / 0x0011 | U32 | 0.1 | VA | Phase R power |
| Vac2 (S) | 0x0012 | U16 | 0.1 | V | |
| Iac2 (S) | 0x0013 | U16 | 0.1 | A | |
| Pac2 H / L | 0x0014 / 0x0015 | U32 | 0.1 | VA | |
| Vac3 (T) | 0x0016 | U16 | 0.1 | V | |
| Iac3 (T) | 0x0017 | U16 | 0.1 | A | |
| Pac3 H / L | 0x0018 / 0x0019 | U32 | 0.1 | VA | |
| IPF (output PF) | 0x002D | U16 | 0.0001 | — | 0–20000, 10000 = 1.0 |
| Rac H / L | 0x003A / 0x003B | U32 | 0.1 | Var | AC reactive power |

### 3.4 Energy

| Name | Addr | Type | Scale | Unit |
|------|------|------|-------|------|
| Energy today H / L | 0x001A / 0x001B | U32 | 0.1 | kWh |
| Energy total H / L | 0x001C / 0x001D | U32 | 0.1 | kWh |
| Time total H / L | 0x001E / 0x001F | U32 | 0.5 | s |
| Epv1 today H / L | 0x0030 / 0x0031 | U32 | 0.1 | kWh |
| Epv1 total H / L | 0x0032 / 0x0033 | U32 | 0.1 | kWh |
| Epv2 today H / L | 0x0034 / 0x0035 | U32 | 0.1 | kWh |
| Epv2 total H / L | 0x0036 / 0x0037 | U32 | 0.1 | kWh |
| Epv total H / L | 0x0038 / 0x0039 | U32 | 0.1 | kWh |
| Epv3 today H / L | 0x007C / 0x007D | U32 | 0.1 | kWh |
| Epv3 total H / L | 0x007E / 0x007F | U32 | 0.1 | kWh |
| E_rac today H / L | 0x003C / 0x003D | U32 | 0.1 | kVarh |
| E_rac total H / L | 0x003E / 0x003F | U32 | 0.1 | kVarh |

### 3.5 Temperature & Fault values

| Name | Addr | Type | Scale | Unit |
|------|------|------|-------|------|
| Temperature (inverter) | 0x0020 | U16 | 0.1 | °C |
| IPM Temperature | 0x0029 | U16 | 0.1 | °C |
| ISO fault Value | 0x0021 | U16 | 0.1 | V |
| GFCI fault Value | 0x0022 | U16 | 1 | mA |
| DCI fault Value | 0x0023 | U16 | 0.01 | A |
| Vpv fault Value | 0x0024 | U16 | 0.1 | V |
| Vac fault Value | 0x0025 | U16 | 0.1 | V |
| Fac fault Value | 0x0026 | U16 | 0.01 | Hz |
| Temperature fault Value | 0x0027 | U16 | 0.1 | °C |
| P Bus Voltage | 0x002A | U16 | 0.1 | V |
| N Bus Voltage | 0x002B | U16 | 0.1 | V |

## 4. Inverter Status (Input 0x0000) — Mode Map

| Value | Meaning |
|-------|---------|
| 0 | Waiting (standby) |
| 1 | Normal (on-grid) |
| 3 | Fault |

Note: Growatt V3.14 defines only 3 codes. Map to RTU `InverterMode` as:
INITIAL/STANDBY → 0, ON_GRID → 1, FAULT → 3, (OFF_GRID/SHUTDOWN → NOT FOUND).

### Fault / Warning codes

| Name | Addr | FC | Notes |
|------|------|----|----|
| Fault code | 0x0028 | 04 | Bit field, see &*1 |
| Faultcode H / L | 0x0080 / 0x0081 | 04 | 32-bit fault code (&*8 transfer) added in V3.13 |
| WarningCode | 0x0040 | 04 | Bit field &*8 |
| WarningValue1 (slave CPU) | 0x0041 | 04 | |
| WarningValue2 (main CPU) | 0x0045 | 04 | PV1ShortCircuit=0x0001, PV2ShortCircuit=0x0002, BT1DriverFault=0x0004, BT2DriverFault=0x0008 |
| DeratingMode | 0x002F | 04 | 0:none 1:PV 3:Vac 4:Fac 5:Tboost 6:Tinv 7:Control |
| StrFault | 0x0056 | 04 | String fault bitmap |
| StrWarning | 0x0057 | 04 | String warning bitmap |
| StrBreak | 0x0058 | 04 | Bit0-7 = String1~8 disconnect |
| PIDFaultCode | 0x0059 | 04 | |
| Grid Fault record 1..5 | 0x005A – 0x0072 | 04 | 5 records × 5 regs (code/year-month/day-hour/min-sec/value) |

## 5. PV Strings (8 channels available — use 6 for this model)

Input registers; current signed −15A..+15A (S16).

| Name | Addr | Type | Scale | Unit |
|------|------|------|-------|------|
| V_String1 | 0x0046 | U16 | 0.1 | V |
| Curr_String1 | 0x0047 | S16 | 0.1 | A |
| V_String2 | 0x0048 | U16 | 0.1 | V |
| Curr_String2 | 0x0049 | S16 | 0.1 | A |
| V_String3 | 0x004A | U16 | 0.1 | V |
| Curr_String3 | 0x004B | S16 | 0.1 | A |
| V_String4 | 0x004C | U16 | 0.1 | V |
| Curr_String4 | 0x004D | S16 | 0.1 | A |
| V_String5 | 0x004E | U16 | 0.1 | V |
| Curr_String5 | 0x004F | S16 | 0.1 | A |
| V_String6 | 0x0050 | U16 | 0.1 | V |
| Curr_String6 | 0x0051 | S16 | 0.1 | A |
| V_String7 | 0x0052 | U16 | 0.1 | V |
| Curr_String7 | 0x0053 | S16 | 0.1 | A |
| V_String8 | 0x0054 | U16 | 0.1 | V |
| Curr_String8 | 0x0055 | S16 | 0.1 | A |

## 6. Control Registers (Holding, FC03/06/16)

| Name | Addr | Type | Value Range | Unit | Purpose |
|------|------|------|-------------|------|---------|
| OnOff | 0x0000 | U16 | low byte: 0=off / 1=on; high byte: auto-start flag | — | Inverter ON/OFF |
| SPIenable | 0x0001 | U16 | 0 or 1 | — | System Protection Interface (CEI021) |
| PF CMD memory state | 0x0002 | U16 | 0 or 1 | — | Whether PF/ActivePRate settings persist |
| Active P Rate | 0x0003 | U16 | 0 – 100 | % | Max output active power limit |
| Reactive P Rate | 0x0004 | U16 | 0 – 100 | % | Max output reactive power limit |
| Power factor | 0x0005 | U16 | 0 – 20000 | ×10000 | 0-10000 = underexcited, 10000-20000 = overexcited, 10000 = 1.0 |
| Pmax H / L | 0x0006 / 0x0007 | U32 | — | 0.1 VA | Nominal power |
| Vnormal | 0x0008 | U16 | — | 0.1 V | Normal work PV voltage |
| Vpv start | 0x0011 | U16 | — | 0.1 V | PV start voltage |
| Time start | 0x0012 | U16 | — | 1 s | Start delay |
| Vac low / high | 0x0013 / 0x0014 | U16 | — | 0.1 V | Grid V limit 1 |
| Fac low / high | 0x0015 / 0x0016 | U16 | — | 0.01 Hz | Grid F limit 1 |
| Vac low2 / high2 | 0x0023 / 0x0024 | U16 | — | 0.1 V | Grid V limit 2 |
| Fac low2 / high2 | 0x0025 / 0x0026 | U16 | — | 0.01 Hz | Grid F limit 2 |
| Vac lowC / highC | 0x0027 / 0x0028 | U16 | — | 0.1 V | V limit for grid reconnect |
| Fac lowC / highC | 0x0029 / 0x002A | U16 | — | 0.01 Hz | F limit for grid reconnect |
| Reset User Info | 0x0020 | U16 | 0x0001 | — | Reset user data |
| Reset to factory | 0x0021 | U16 | 0x0001 | — | Factory reset |

### Control summary per RTU control types

| RTU control | Register | Encoding |
|------|------|----|
| ON/OFF | 0x0000 (OnOff) | 0x0001 = ON, 0x0000 = OFF (low byte); typical write 0x0101 for ON+autostart |
| Active power limit | 0x0003 (Active P Rate) | 0–100 % direct |
| Reactive power limit | 0x0004 (Reactive P Rate) | 0–100 % direct |
| Power factor | 0x0005 (Power factor) | PF×10000, range 0–20000 |
| Grid reconnect V/F | 0x0027–0x002A | Limit thresholds |

## 7. Items NOT FOUND in this PDF

- **No DER-AVM / Action Mode register** — Growatt PV protocol V3.14 has no explicit action-mode register; fallback uses OnOff + ActivePRate + PF.
- **No dedicated PowerFactor sign register** — encoded within 0x0005 by under/overexcited split.
- **No Off-Grid / Shutdown mode code** in Inverter Status (only 0/1/3).
- **No explicit OFF-GRID or BACKUP voltage/current registers** (this is PV-only protocol, not hybrid).
- **ErrorCode table bit definitions** — Fault code is bit-field but the per-bit meaning table is referenced as `&*1` and only partially listed in this PDF version; use Growatt fault code reference doc for full bit-to-text mapping.

## 8. Recommended RTU integration notes

1. Use **FC04** for all real-time data (section 3), **FC03/06/16** for control (section 6).
2. Split reads into ≤45-register batches aligned on 45-word boundaries (PDF requirement).
3. Respect **≥850 ms** between requests per slave.
4. 30 kW MID-25~40KTL3-X: **MPPT=3**, **Strings=6**, **Phase=3** → read V_String1..6 only (0x0046–0x0051).
5. Confirm `TP` register (0x002C) returns `0x0303` at runtime to auto-detect the topology.
