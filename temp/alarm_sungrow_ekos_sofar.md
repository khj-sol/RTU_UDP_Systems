# Alarm/Fault Bit Field Definitions: Sungrow, Ekos, Sofar

## 1. Sungrow (Sungrow_50_3_registers.py)

- **ERROR_CODE1/2/3** = addr 0x5045 (all alias same register)
- **Type**: Enumerated fault code (NOT bitfield)
- PDF Appendix 3: decimal code -> fault description (e.g., 002=Grid overvoltage, 012=Excessive leakage current)
- **BITS dict: N/A** -- Sungrow uses single U16 enum value, not bit flags.
- 현재 `BITS = {}` 유지가 맞음. 별도 FAULT_CODE_TABLE dict 추가 권장.

### Sungrow Fault Code Table (Appendix 3 excerpt)

| Code | Hex    | Description                        |
|------|--------|------------------------------------|
| 002  | 0x0002 | Grid overvoltage                   |
| 003  | 0x0003 | Grid transient overvoltage         |
| 004  | 0x0004 | Grid undervoltage                  |
| 005  | 0x0005 | Grid low voltage                   |
| 007  | 0x0007 | AC instantaneous overcurrent       |
| 008  | 0x0008 | Grid over frequency                |
| 009  | 0x0009 | Grid underfrequency                |
| 010  | 0x000A | Grid power outage                  |
| 011  | 0x000B | Device abnormal                    |
| 012  | 0x000C | Excessive leakage current          |
| 013  | 0x000D | Grid abnormal                      |
| 014  | 0x000E | 10-minute grid overvoltage         |
| 015  | 0x000F | Grid high voltage                  |
| 016  | 0x0010 | Output overload                    |
| 017  | 0x0011 | Grid voltage unbalance             |
| 019  | 0x0013 | Device abnormal                    |
| 020  | 0x0014 | Device abnormal                    |
| 021  | 0x0015 | Device abnormal                    |
| 022  | 0x0016 | Device abnormal                    |
| 023  | 0x0017 | PV connection fault                |
| 036  | 0x0024 | Module temperature too high        |
| 037  | 0x0025 | Ambient temperature too high       |
| 039  | 0x0027 | Low system insulation resistance   |
| 043  | 0x002B | Low ambient temperature            |
| 047  | 0x002F | PV input config abnormal           |

---

## 2. Ekos (Ekos_10_3_registers.py)

ErrorCode1 (F010, SW Fault) / ErrorCode2 (F011, HW Fault): **이미 채워져 있음**.
ErrorCode3 = GRID_STATUS_ALARM3 (reg 30103): `BITS = {}` -- Fault MAP 시트에서 추출.

### ErrorCode3 (GRID_STATUS_ALARM3, reg 30103) -- Fault MAP "계통(102)"

| Bit | Name           | Description (KR)  |
|-----|----------------|--------------------|
| 0   | GridOVR        | 계통 과전압          |
| 1   | GridUVR        | 계통 저전압          |
| 2   | GridOFR        | 계통 과주파수         |
| 3   | GridUFR        | 계통 저주파수         |
| 4   | GridFAIL       | 계통정전             |
| 5   | GridEARTH      | 계통지락             |
| 6   | GridRCMU       | 계통잔류전류          |
| 11  | GridSPD        | 계통SPD이상          |
| 15  | GridFaultAll   | 전체 상태 (any fault)|

### 참고: 추가 Fault MAP 레지스터 (현재 코드에 미사용)

**PV_STATUS_TOTAL (30099)**: bit0=PV과전압, bit2=PV저전압, bit11=PV SPD, bit13=DC지락, bit14=비상스위치, bit15=전체상태
**INVERTER_STATUS_FAULT (30102)**: bit2=과전류Trip(HW), bit3=과전류(SW), bit7=과열, bit8=MC이상, bit10=PWM이상, bit11=DC-Link불균형, bit15=전체상태
**CONVERTER_STATUS (30104)**: bit0=과전류HW, bit1=센서offset, bit4=과전압SW, bit5=PWM이상, bit15=전체상태

---

## 3. Sofar (Sofar_50_3_registers.py)

- FAULT1~FAULT5 (0x0001~0x0005): 5 x U16 = 80 bits (10 bytes)
- INVERTER_ALERT (0x0021): 1 x U16 = 16 bits
- **Type**: Bitfield (byte0=FAULT1 low byte, byte1=FAULT1 high byte, ...)

### ErrorCode1 (FAULT1, addr 0x0001) -- 16 bits

| Bit | Name                 | Description                          |
|-----|----------------------|--------------------------------------|
| 0   | GridOVP              | Grid Over Voltage Protection         |
| 1   | GridUVP              | Grid Under Voltage Protection        |
| 2   | GridOFP              | Grid Over Frequency Protection       |
| 3   | GridUFP              | Grid Under Frequency Protection      |
| 4   | PVUVP                | PV Under Voltage Protection          |
| 5   | GridLVRT             | Grid Low Voltage Ride Protection     |
| 8   | PVOVP                | PV Over Voltage Protection           |
| 9   | IpvUnbalance         | PV Input Current Unbalance           |
| 10  | PvConfigSetWrong     | PV Input Mode Configure Wrong        |
| 11  | GFCIFault            | GFCI Fault                           |
| 12  | PhaseSequenceFault   | Phase Sequence Fault                 |
| 13  | HwBoostOCP           | HW Boost Over Current Protection     |
| 14  | HwAcOCP              | HW AC Over Current Protection        |
| 15  | AcRmsOCP             | Grid Current Too High                |

### ErrorCode2 (FAULT2, addr 0x0002) -- 16 bits

| Bit | Name                 | Description                          |
|-----|----------------------|--------------------------------------|
| 0   | HwADFaultIGrid       | Grid Current Sampling Fault          |
| 1   | HwADFaultDCI         | DCI Sampling Fault                   |
| 2   | HwADFaultVGrid       | Grid Voltage Sampling Fault          |
| 3   | GFCIDeviceFault      | GFCI Device Sampling Fault           |
| 4   | MChipFault           | Main Chip Fault                      |
| 5   | HwAuxPowerFault      | HW Auxiliary Power Fault             |
| 6   | BusVoltZeroFault     | BUS Voltage Zero Fault               |
| 7   | IacRmsUnbalance      | Unbalance Output Current             |
| 8   | BusUVP               | Bus Under Voltage Protection         |
| 9   | BusOVP               | Bus Over Voltage Protection          |
| 10  | VbusUnbalance        | Bus Voltage Unbalance                |
| 11  | DciOCP               | DCI Over Current Protection          |
| 12  | SwOCPInstant         | Grid Current Too High (SW)           |
| 13  | SwBOCPInstant        | Input Current Too High (SW)          |

### ErrorCode3 (FAULT3, addr 0x0003) -- 16 bits

| Bit | Name                      | Description                          |
|-----|---------------------------|--------------------------------------|
| 0   | ConsistentFault_VGrid     | Grid V sampling consistency error    |
| 1   | ConsistentFault_FGrid     | Grid F sampling consistency error    |
| 2   | ConsistentFault_DCI       | DCI sampling consistency error       |
| 3   | ConsistentFault_GFCI      | GFCI sampling consistency error      |
| 4   | SpiCommLose               | Master-slave DSP comm fail           |
| 5   | SciCommLose               | Slave-comm board comm fail           |
| 6   | RelayTestFail             | Relay Fault                          |
| 7   | PvIsoFault                | Low PV insulation resistance         |
| 8   | OverTempFault_Inv         | Inverter temp too high               |
| 9   | OverTempFault_Boost       | Boost temp too high                  |
| 10  | OverTempFault_Env         | Environment temp too high            |
| 11  | PEConnectFault            | No PE wire connected                 |

### FAULT4 (addr 0x0004) -- 16 bits

| Bit | Name                       | Description                          |
|-----|----------------------------|--------------------------------------|
| 0   | unrecoverHwAcOCP           | Unrecoverable grid overcurrent       |
| 1   | unrecoverBusOVP            | Unrecoverable bus overvoltage        |
| 2   | unrecoverIacRmsUnbalance   | Unrecoverable grid unbalance         |
| 3   | unrecoverIpvUnbalance      | Unrecoverable PV input unbalance     |
| 4   | unrecoverVbusUnbalance     | Unrecoverable bus V unbalance        |
| 5   | unrecoverOCPInstant        | Unrecoverable grid overcurrent       |
| 6   | unrecoverPvConfigSetWrong  | Unrecoverable PV config wrong        |
| 9   | unrecoverIPVInstant        | Unrecoverable input overcurrent      |
| 10  | unrecoverWRITEEEPROM       | EEPROM write fault                   |
| 11  | unrecoverREADEEPROM        | EEPROM read fault                    |
| 12  | unrecoverRelayFail         | Unrecoverable relay fault            |

### INVERTER_ALERT (addr 0x0021) -- 16 bits

| Bit | Name              | Description                          |
|-----|-------------------|--------------------------------------|
| 0   | OverTempDerating  | Derated due to high temperature      |
| 1   | OverFreqDerating  | Derated due to high grid frequency   |
| 2   | RemoteDerating    | Derated by remote control            |
| 3   | RemoteOff         | Shut down by remote control          |
| 4   | UnderFreqDerate   | Derated due to low grid frequency    |
