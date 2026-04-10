# Sunways PV Inverter Modbus Map (STT-30KTL, 30kW 3-phase, 3 MPPT, 6 strings)

Source: `Sunways-PV_879922609-Modbus-Protocol.pdf` (Version 00.07, 2023-07-06)

## Conventions

- Physical: RS485, 9600 bps, 8N1, no parity
- Slave address: 1~247 (broadcast 0); default per device, configurable
- Byte order: **Big-endian** ("MSB sent first"); for U32/I32 = HL word order (high register first at lower address)
- Function codes: FC03 (read holding), FC06 (write single), FC16 (write multiple)
- Register addresses below are decimal as in the PDF; hex shown alongside
- Data types: U16, I16, U32, I32, STR; "Gain" = scale factor (raw / gain = engineering value)
- Model identification: register 10008 → high byte = MPPT type (16 = Three-phase Four-MPPT), low byte = rating index (1 = STT-30KTL)

## 1. Device Info (FC03)

| Address (hex) | Dec | Name | FC | DataType | Words | Scale | Unit | Notes |
|---|---|---|---|---|---|---|---|---|
| 0x2710 | 10000 | Serial Number | 3 | STRING | 8 | - | - | 16 chars, hi/lo bytes |
| 0x2718 | 10008 | Model Information | 3 | U16 | 1 | - | - | hi=type(16), lo=rating(1=STT-30KTL) |
| 0x2719 | 10009 | Output Mode | 3 | U16 | 1 | - | - | 0=3P4W, 1=3P3W |
| 0x271A | 10010 | Protocol Version | 3 | U16 | 1 | - | - | currently 0 |
| 0x271B | 10011 | Firmware Version | 3 | U32 | 2 | - | - | byte parsing, HL |

## 2. AC Measurements (FC03)

| Address (hex) | Dec | Name | FC | DataType | Words | Scale | Unit | Notes |
|---|---|---|---|---|---|---|---|---|
| 0x2AFE | 11006 | Grid Line Voltage AB | 3 | U16 | 1 | 10 | V | |
| 0x2AFF | 11007 | Grid Line Voltage BC | 3 | U16 | 1 | 10 | V | |
| 0x2B00 | 11008 | Grid Line Voltage CA | 3 | U16 | 1 | 10 | V | |
| 0x2B01 | 11009 | Grid Phase A Voltage | 3 | U16 | 1 | 10 | V | |
| 0x2B02 | 11010 | Grid Phase A Current | 3 | U16 | 1 | 10 | A | |
| 0x2B03 | 11011 | Grid Phase B Voltage | 3 | U16 | 1 | 10 | V | |
| 0x2B04 | 11012 | Grid Phase B Current | 3 | U16 | 1 | 10 | A | |
| 0x2B05 | 11013 | Grid Phase C Voltage | 3 | U16 | 1 | 10 | V | |
| 0x2B06 | 11014 | Grid Phase C Current | 3 | U16 | 1 | 10 | A | |
| 0x2B07 | 11015 | Grid Frequency | 3 | U16 | 1 | 100 | Hz | |
| 0x2B08 | 11016 | P_AC (Total Active Power) | 3 | U32 | 2 | 1000 | kW | HL |
| 0x2B0A | 11018 | Daily Energy | 3 | U32 | 2 | 10 | kWh | HL |
| 0x2B0C | 11020 | Total Energy (Lifetime) | 3 | U32 | 2 | 10 | kWh | HL |
| 0x2B0E | 11022 | Total Generating Time | 3 | U32 | 2 | 1 | h | HL |
| 0x2B10 | 11024 | Output Apparent Power | 3 | U32 | 2 | 1000 | kVA | HL |
| 0x2B12 | 11026 | Output Reactive Power | 3 | I32 | 2 | 1000 | kvar | HL, signed |
| 0x2B14 | 11028 | Total Power Input | 3 | U32 | 2 | 1000 | kW | HL, total PV input |
| 0x2B16 | 11030 | Power Factor | 3 | I16 | 1 | 1000 | - | signed, -1.000~1.000 |
| 0x2B17 | 11031 | Efficiency | 3 | U16 | 1 | 100 | % | |

## 3. Inverter Status / State

| Address (hex) | Dec | Name | FC | DataType | Words | Scale | Unit | Notes |
|---|---|---|---|---|---|---|---|---|
| 0x2779 | 10105 | Working State | 3 | U16 | 1 | - | - | see state table |
| 0x2780 | 10112 | Fault FLAG1 | 3 | U32 | 2 | - | - | bitwise (Table 3.3) |
| 0x2782 | 10114 | Fault FLAG2 | 3 | U32 | 2 | - | - | bitwise |
| 0x2788 | 10120 | Fault FLAG3 | 3 | U32 | 2 | - | - | bitwise |

State codes (10105):
| Value | Meaning |
|---|---|
| 0 | wait — Waiting for grid |
| 1 | check — Self-inspection |
| 2 | normal — Generating |
| 3 | fault — Device failure |
| 4 | flash — Firmware update |

## 4. Per-MPPT (PV1~PV3 used for 3-MPPT model)

| Address (hex) | Dec | Name | FC | DataType | Words | Scale | Unit | Notes |
|---|---|---|---|---|---|---|---|---|
| 0x2B26 | 11038 | PV1 Voltage | 3 | U16 | 1 | 10 | V | |
| 0x2B27 | 11039 | PV1 Current | 3 | U16 | 1 | 10 | A | |
| 0x2B28 | 11040 | PV2 Voltage | 3 | U16 | 1 | 10 | V | |
| 0x2B29 | 11041 | PV2 Current | 3 | U16 | 1 | 10 | A | |
| 0x2B2A | 11042 | PV3 Voltage | 3 | U16 | 1 | 10 | V | |
| 0x2B2B | 11043 | PV3 Current | 3 | U16 | 1 | 10 | A | |
| 0x2B3E | 11062 | PV1 Input Power | 3 | U32 | 2 | 1000 | kW | HL |
| 0x2B40 | 11064 | PV2 Input Power | 3 | U32 | 2 | 1000 | kW | HL |
| 0x2B42 | 11066 | PV3 Input Power | 3 | U32 | 2 | 1000 | kW | HL |

(PV4~PV10 voltage/current at 11044~11081, PV4~PV10 power at 11068~11097 — unused for 3-MPPT model.)

## 5. String Currents (6 strings, 2 per MPPT)

| Address (hex) | Dec | Name | FC | DataType | Words | Scale | Unit | Notes |
|---|---|---|---|---|---|---|---|---|
| 0x2B32 | 11050 | String1 Current | 3 | U16 | 1 | 10 | A | MPPT1-A |
| 0x2B33 | 11051 | String2 Current | 3 | U16 | 1 | 10 | A | MPPT1-B |
| 0x2B34 | 11052 | String3 Current | 3 | U16 | 1 | 10 | A | MPPT2-A |
| 0x2B35 | 11053 | String4 Current | 3 | U16 | 1 | 10 | A | MPPT2-B |
| 0x2B36 | 11054 | String5 Current | 3 | U16 | 1 | 10 | A | MPPT3-A |
| 0x2B37 | 11055 | String6 Current | 3 | U16 | 1 | 10 | A | MPPT3-B |

(String7~String20 at 11056~11089 exist but unused for 6-string model.)

## 6. Temperature

| Address (hex) | Dec | Name | FC | DataType | Words | Scale | Unit | Notes |
|---|---|---|---|---|---|---|---|---|
| 0x2B18 | 11032 | Temperature1 | 3 | I16 | 1 | 10 | degC | typically heatsink/IGBT |
| 0x2B19 | 11033 | Temperature2 | 3 | I16 | 1 | 10 | degC | |
| 0x2B1A | 11034 | Temperature3 | 3 | I16 | 1 | 10 | degC | |
| 0x2B1B | 11035 | Temperature4 | 3 | I16 | 1 | 10 | degC | |
| 0x2B1C | 11036 | BUS Voltage | 3 | U16 | 1 | 10 | V | DC bus |
| 0x2B1D | 11037 | NBS Voltage | 3 | U16 | 1 | 10 | V | neutral bus |

## 7. Control Registers (FC06 / FC16)

| Address (hex) | Dec | Name | FC | DataType | Words | Scale | Unit | Notes |
|---|---|---|---|---|---|---|---|---|
| 0x61B0 | 25008 | Switch On/Off Setting | 06 | U16 | 1 | 1 | - | BIT0: 1=on/0=off; BIT1: 1=restart |
| 0x6218 | 25112 | Active Power Limit | 16 | U32 | 2 | 1 | W | WO, HL |
| 0x621A | 25114 | Active Power Limit Percentage | 06 | U16 | 1 | 1000 | - | WO, [0,1000] = 0~100% |
| 0x621C | 25116 | Reactive Power Limit | 16 | I32 | 2 | 1 | var | WO, HL, signed |
| 0x621E | 25118 | Reactive Power Limit Percentage | 06 | I16 | 1 | 1000 | - | WO, [-600,+600] |
| 0x6220 | 25120 | Power Factor Setting | 06 | I16 | 1 | 1000 | - | RW, (-1000,-800] U [800,1000] |
| 0x4E20 | 20000 | RTC Year/Month | 06 | U16 | 1 | 1 | - | RW |
| 0x4E21 | 20001 | RTC Day/Hour | 06 | U16 | 1 | 1 | - | RW |
| 0x4E22 | 20002 | RTC Minute/Second | 06 | U16 | 1 | 1 | - | RW |

Notes:
- **Grid-tie remote on/off**: use register 25008 (FC06). BIT0=1 turn on, BIT0=0 turn off, BIT1=1 restart.
- **Active power %**: write 0~1000 to 25114 (FC06) for 0~100.0%.
- **Reactive power %**: write -600~+600 to 25118 (FC06) for -60.0~+60.0%.
- **Power factor**: write -1000~+1000 (excluding (-800,800)) to 25120.
- No dedicated "remote enable" register found beyond 25008 switch; control commands take effect directly.

## Word Order Summary

- All multi-register U32/I32 values: **HL** (high word at lower register address, big-endian within each word)
- No Float32 values used in this protocol
- STR (serial number) parsed as ASCII characters in high/low bytes

## Slave ID

Configurable 1~247; broadcast = 0; 248~255 reserved. Default not specified in PDF (typically 1).
