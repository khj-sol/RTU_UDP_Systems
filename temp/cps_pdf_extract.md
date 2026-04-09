# CPS-PV / CPS-SCA-M Modbus Protocol V4.16 — Register Reference

**Source:** CPS-PV_CPS-SCA-M_Modbus_Protocol_V4.16_EN.pdf
**Target:** CPS 50kW 3-phase string inverter, 3 MPPT, 9 strings (3 per MPPT)

## Communication Settings
- Mode: Modbus RTU, Half duplex
- Baud: 9600, 8-N-1
- Slave ID range: 1~247 (single host can connect up to 247 inverters)
- Function codes: 0x03 (read), 0x06 (write single), 0x10 (write multiple)
- Word order for U32 / I32: **HL (high word at lower address, big-endian)** — confirmed from history-log example (0xB0002 = high word of error code, 0xB0003 = low word)

## 1. Device Information (FC03)

| Address | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x1A00 | Device Model | 3 | STRING (8 regs / 16 chars) | - | ASCII | e.g. "PV 30KTL" |
| 0x1A10 | Device Serial Number | 3 | STRING (8 regs / 16 chars) | - | ASCII | e.g. "1234-123456789" |
| 0x1A18 | Modbus Protocol Version | 3 | U16 | - | - | 0x1234 = V12.34 |
| 0x1A1C | Software Version | 3 | STRING (3 regs / 6 chars) | - | ASCII | |
| 0x1A23 | Software Build Date | 3 | STRING (3 regs / 6 chars) | - | ASCII | |
| 0x1A3B | MPPT Number | 3 | U16 | - | - | 1 = 1 MPPT |
| 0x1A44 | Rated Voltage | 3 | U16 | 0.1 | V | |
| 0x1A45 | Rated Frequency | 3 | U16 | 0.01 | Hz | |
| 0x1A46 | Rated Power | 3 | U16 | 1 | W | |
| 0x1A48 | Grid Phase Number | 3 | U16 | - | - | 1=single, 3=three-phase (50kW=3) |

## 2. AC Real-Time Data (FC03)

| Address | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x1001 | Phase A Voltage | 3 | U16 | 0.1 | V | |
| 0x1002 | Phase A Current | 3 | U16 | 0.01 | A | |
| 0x1003 | Phase A Power | 3 | U32 | 0.1 | W | HL |
| 0x1005 | Phase A Frequency | 3 | U16 | 0.01 | Hz | |
| 0x1006 | Phase B Voltage | 3 | U16 | 0.1 | V | |
| 0x1007 | Phase B Current | 3 | U16 | 0.01 | A | |
| 0x1008 | Phase B Power | 3 | U32 | 0.1 | W | HL |
| 0x100A | Phase B Frequency | 3 | U16 | 0.01 | Hz | |
| 0x100B | Phase C Voltage | 3 | U16 | 0.1 | V | |
| 0x100C | Phase C Current | 3 | U16 | 0.01 | A | |
| 0x100D | Phase C Power | 3 | U32 | 0.1 | W | HL |
| 0x100F | Phase C Frequency | 3 | U16 | 0.01 | Hz | |
| 0x1037 | Total Active Power | 3 | U32 | 0.1 | W | HL |
| 0x1039 | Total Reactive Power | 3 | S32 | 0.1 | Var | HL, signed |
| 0x103D | Power Factor | 3 | S16 | 0.001 | - | signed |
| 0x1021 | Total Energy (lifetime) | 3 | U32 | 1 | kWh | HL |
| 0x1023 | Total Generation Time | 3 | U32 | 1 | hour | HL |
| 0x1027 | Today Energy | 3 | U32 | 1 | Wh | HL |
| 0x103B | Today Peak Power | 3 | U32 | 0.1 | W | HL |

## 3. Inverter Status / Error

| Address | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x101D | Inverter Mode | 3 | U16 | - | - | See mode table below |
| 0x101E | Error Code | 3 | U32 | - | - | HL, bitmask, see error table |

### Inverter Mode Table (0x101D)
| Value | Mode |
|---|---|
| 0x00 | Initial mode |
| 0x01 | Standby mode |
| 0x03 | Online mode (On-Grid) |
| 0x05 | Fault mode |
| 0x09 | Shutdown mode |

### Error Code Bits (0x101E, U32) — selected
| Bit | Description | Type |
|---|---|---|
| 0 | Grid AC over voltage | Notice |
| 1 | Grid AC under voltage | Notice |
| 2 | Grid AC absent | Notice |
| 3 | Grid AC over frequency | Notice |
| 4 | Grid AC under frequency | Notice |
| 5 | PV DC over voltage | Notice |
| 6 | PV insulation abnormal | Notice |
| 7 | Leakage current abnormal | Notice |
| 9 | Control power low | Fault |
| 10 | PV string abnormal | Notice |
| 11 | PV DC under voltage | Notice |
| 14 | Arc fault detection | Fault |
| 15 | Ground current > 300mA | Notice |
| 17 | Inverter relay abnormal | Fault |
| 19 | Inverter over temperature | Notice |
| 21 | PV string reverse | Notice |
| 23 | Fan lock | Notice |
| 24 | Bus under voltage | Notice |
| 25 | Bus over voltage | Notice |
| 28 | EEPROM error | Fault |
| 30 | Inverter abnormal | Fault |
| 31 | Boost abnormal | Fault |

## 4. Per-MPPT (PV1/PV2/PV3) — FC03

| Address | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x1010 | PV1 Voltage | 3 | U16 | 0.1 | V | |
| 0x1011 | PV1 Current | 3 | U16 | 0.01 | A | |
| 0x1012 | MPPT1 Power | 3 | U32 | 0.1 | W | HL |
| 0x1014 | PV2 Voltage | 3 | U16 | 0.1 | V | |
| 0x1015 | PV2 Current | 3 | U16 | 0.01 | A | |
| 0x1016 | MPPT2 Power | 3 | U32 | 0.1 | W | HL |
| 0x1018 | PV3 Voltage | 3 | U16 | 0.1 | V | |
| 0x1019 | PV3 Current | 3 | U16 | 0.01 | A | |
| 0x101A | MPPT3 Power | 3 | U32 | 0.1 | W | HL |
| 0x103E | PV4 Voltage | 3 | U16 | 0.1 | V | (Note1, optional) |
| 0x103F | PV4 Current | 3 | U16 | 0.01 | A | (Note1, optional) |
| 0x1040 | MPPT4 Power | 3 | U32 | 0.1 | W | HL (Note1, optional) |

## 5. Per-String Currents (9 strings)

**NOT FOUND** — Modbus Protocol V4.16 does not expose individual PV string currents. Only per-MPPT aggregate current (PV1/PV2/PV3 Current at 0x1011/0x1015/0x1019) is available. There is a "PV String detection" enable flag (0x5111) but no per-string measurement registers in this document.

## 6. Temperature

| Address | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x101C | Inner Temperature | 3 | S16 | 1 | °C | signed; sole temperature register (no separate heatsink) |

## 7. Control Registers (FC06 / FC16)

| Address | Name | FC | DataType | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x6001 | Inverter Control (power on/off) | 6 | U16 | - | - | 0=power on, 1=shut down (WO) |
| 0x5104 | Active Power Limit (Derating Watt Percent) | 6 | U16 | 1 | % | [10, 1000] = 1.0~100.0% of rated (RW) |
| 0x5114 | Reactive Power Percent | 6 | U16 | 1 | % | [1, 1000] of rated (RW, Note1) |
| 0x5031 | Power Factor Setting | 6 | S16 | 0.001 | pf | [-1000,-800] ∪ [800,1000] |
| 0x5101 | Regulation Code (grid code) | 6 | U16 | - | - | See Regulation table |
| - | Grid-tie remote control | - | - | - | - | Use 0x6001 (power on / shut down). No separate "grid-tie" register defined. |

### Date/Time setting (FC06, RW)
| Address | Name | DataType | Notes |
|---|---|---|---|
| 0x3000 | Year | U16 | 0x07E1 = 2017 |
| 0x3001 | Month+Day | U16 | High=Month, Low=Day |
| 0x3002 | Hour+Minute | U16 | High=Hour, Low=Minute |
| 0x3003 | Second+0 | U16 | High=Second, Low=0 |

### Other Parameters (RW, FC06) — partial
| Address | Name | Range | Unit |
|---|---|---|---|
| 0x5000 | Soft start time | [10,600] | s |
| 0x5001 | Reconnect time | [10,900] | s |
| 0x5002 | Grid freq high loss L1 limit | [1,1.2]*rated | 0.01Hz |
| 0x5003 | Grid freq low loss L1 limit | [0.8,1]*rated | 0.01Hz |
| 0x5004 | Grid volt high loss L1 limit | [1,1.36]*rated | 0.1V |
| 0x5005 | Grid volt low loss L1 limit | [0.3,1]*rated | 0.1V |
| 0x510E | Islanding detection | 0/1 | - |
| 0x5111 | PV String detection | 0/1 | - |
| 0x5118 | Ground Current Detection | 0/1 | - |

## Notes
- **Word order**: U32/S32 stored as HL (big-endian word order). Confirmed via history log example.
- **Slave ID**: 1~247, set physically per inverter. CPS 50kW 3-phase confirmed via 0x1A48 = 3.
- **Note1** registers (PV4, Reactive Power Percent, etc.) only present on newer firmware/models — verify with target unit.
- **Per-string currents (9 strings)**: NOT in this protocol document. CPS-SCA-M 50kW reports only 3 MPPT aggregate currents.
