# Sofar PV 1-70KTL G1/G2 — Modbus Register Reference

Source: SOFAR-PV_SOFAR_1-70KTL_G1-G2_Modbus_Protocol_EN_2021-01-27.pdf (v1.01, 2019-06-17)
Target: Sofar 50kW 3-phase, 4 MPPT, 8 strings (2/MPPT)

## Conventions

- **Slave address**: 1–63 (broadcast = 0x88)
- **Baud**: 9600 8N1, RS485 half-duplex
- **Function codes**:
  - FC 0x03 → read inverter input registers (0x0000–0x00FF)
  - FC 0x03 → read built-in combiner registers (0x0100–0x01FF)
  - FC 0x04 → inverter holding registers (0x1000–0x10FF) [setting registers]
  - FC 0x06 → real-time power limit (single register write)
  - **Control (ON/OFF, Pset, PFset, Qset)** uses **broadcast 0x88 + FC 0x01** (custom), NOT standard FC06/FC16
- **Word order (32-bit values)**: **HL** — high-byte register at lower address (e.g. 0x0015 = high word, 0x0016 = low word for Total production)
- **All values are Uint16 unless noted; signed marked Int16**

---

## 1. Device Info (model/SN/firmware)

**NOT FOUND** — No model name, serial number, or firmware version registers documented in this PDF.

---

## 2. AC Output

| Address | Name | FC | Type | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x000F | A-phase voltage | 03 | U16 | 0.1 | V | |
| 0x0011 | B-phase voltage | 03 | U16 | 0.1 | V | |
| 0x0013 | C-phase voltage | 03 | U16 | 0.1 | V | |
| 0x0010 | A-phase current | 03 | U16 | 0.01 | A | |
| 0x0012 | B-phase current | 03 | U16 | 0.01 | A | |
| 0x0014 | C-phase current | 03 | U16 | 0.01 | A | |
| 0x000E | Grid frequency | 03 | U16 | 0.01 | Hz | |
| 0x000C | Active power (output) | 03 | U16 | 0.01 | kW | Total |
| 0x000D | Reactive power (output) | 03 | U16 | 0.01 | kVar | Total |
| 0x0015 | Total production (high word) | 03 | U16 | 1 | kWh | HL pair with 0x0016 → U32 |
| 0x0016 | Total production (low word) | 03 | U16 | 1 | kWh | |
| 0x0019 | Daily energy | 03 | U16 | 0.01 | kWh | |

**Power factor**: NOT FOUND as a monitoring register (only as a control setpoint via broadcast).

---

## 3. Inverter Status / State

| Address | Name | FC | Type | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x0000 | Operating state | 03 | U16 | 1 | — | Low-byte only; see code table |
| 0x0001 | Fault 1 (byte0/byte1) | 03 | U16 | — | bitmap | ID01–ID16 |
| 0x0002 | Fault 2 (byte2/byte3) | 03 | U16 | — | bitmap | ID17–ID32 |
| 0x0003 | Fault 3 (byte4/byte5) | 03 | U16 | — | bitmap | ID33–ID48 |
| 0x0004 | Fault 4 (byte6/byte7) | 03 | U16 | — | bitmap | ID49–ID64 |
| 0x0005 | Fault 5 (byte8/byte9) | 03 | U16 | — | bitmap | ID65–ID80 |
| 0x0021 | Inverter alert message | 03 | U16 | — | bitmap | ID81–ID88 (derating/remote) |

### Operating state codes (0x0000)

| Code | Meaning | InverterMode |
|---|---|---|
| 0x00 | Wait | INITIAL/STANDBY |
| 0x01 | Check (self-test) | INITIAL |
| 0x02 | Normal | ON_GRID |
| 0x03 | Fault | FAULT |
| 0x04 | Permanent (unrecoverable) | FAULT |

---

## 4. Per-MPPT (PV input)

PDF documents **only PV1 and PV2** in the inverter address table. PV3/PV4 registers for the 50KTL 4-MPPT model are **NOT documented in this PDF** — must be obtained from the 50KTL-specific addendum or use combiner-side data.

| Address | Name | FC | Type | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x0006 | PV1 voltage | 03 | U16 | 0.1 | V | |
| 0x0007 | PV1 current | 03 | U16 | 0.01 | A | |
| 0x0008 | PV2 voltage | 03 | U16 | 0.1 | V | |
| 0x0009 | PV2 current | 03 | U16 | 0.01 | A | |
| 0x000A | PV1 power | 03 | U16 | 0.01 | kW | |
| 0x000B | PV2 power | 03 | U16 | 0.01 | kW | |
| — | PV3 voltage / current / power | — | — | — | — | **NOT FOUND** |
| — | PV4 voltage / current / power | — | — | — | — | **NOT FOUND** |

> **Workaround for 4-MPPT 50KTL**: Pair adjacent string voltages from the combiner block (0x0105+, see §5) as a fallback for MPPT3/MPPT4 voltage; compute power from V × I in firmware. Confirm with Sofar that the actual 50KTL G2 firmware supports an extended PV3/PV4 register block.

---

## 5. Per-string current (8 strings) — Built-in Combiner block

Read with FC 0x03 from the combiner address range (0x0100–0x01FF). Voltage and current per string are reported.

| Address | Name | FC | Type | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x0105 | PV1 (String 1) voltage | 03 | U16 | 0.1 | V | |
| 0x0106 | PV1 (String 1) current | 03 | U16 (signed range -20..20) | 0.01 | A | Mapped MPPT1-A |
| 0x0107 | PV2 (String 2) voltage | 03 | U16 | 0.1 | V | |
| 0x0108 | PV2 (String 2) current | 03 | U16 | 0.01 | A | Mapped MPPT1-B |
| 0x0109 | PV3 (String 3) voltage | 03 | U16 | 0.1 | V | |
| 0x010A | PV3 (String 3) current | 03 | U16 | 0.01 | A | Mapped MPPT2-A |
| 0x010B | PV4 (String 4) voltage | 03 | U16 | 0.1 | V | |
| 0x010C | PV4 (String 4) current | 03 | U16 | 0.01 | A | Mapped MPPT2-B |
| 0x010D | PV5 (String 5) voltage | 03 | U16 | 0.1 | V | |
| 0x010E | PV5 (String 5) current | 03 | U16 | 0.01 | A | Mapped MPPT3-A |
| 0x010F | PV6 (String 6) voltage | 03 | U16 | 0.1 | V | |
| 0x0110 | PV6 (String 6) current | 03 | U16 | 0.01 | A | Mapped MPPT3-B |
| 0x0111 | PV7 (String 7) voltage | 03 | U16 | 0.1 | V | |
| 0x0112 | PV7 (String 7) current | 03 | U16 | 0.01 | A | Mapped MPPT4-A |
| 0x0113 | PV8 (String 8) voltage | 03 | U16 | 0.1 | V | |
| 0x0114 | PV8 (String 8) current | 03 | U16 | 0.01 | A | Mapped MPPT4-B |

> Combiner data is only available on models that include the built-in DC combiner (typically 30–70KTL). Verify availability for the specific 50KTL G2 unit.

### Combiner fault registers

| Address | Name | FC | Type | Notes |
|---|---|---|---|---|
| 0x0100 | Combiner Fault 1 | 03 | U16 | byte0/1: PV11–PV24 over-voltage |
| 0x0101 | Combiner Fault 2 | 03 | U16 | byte2/3: PV11–PV24 under-voltage |
| 0x0102 | Combiner Fault 3 | 03 | U16 | byte4/5: PV11–PV24 reflux power |
| 0x0103 | Combiner Fault 4 | 03 | U16 | byte6/7: PV11–PV24 over-current |
| 0x0104 | Combiner Fault 5 | 03 | U16 | byte8/9: PV11–PV24 fuse fault |

---

## 6. Temperature

| Address | Name | FC | Type | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x001B | Inverter module temperature | 03 | U16 | 1 | °C | IGBT/heatsink |
| 0x001C | Inverter inner (ambient) temperature | 03 | U16 | 1 | °C | |
| 0x001D | Inverter Bus voltage | 03 | U16 | 0.1 | V | DC bus |

---

## 7. Control Registers

> **Sofar 1-70KTL does NOT use standard FC06/FC16 for control.** All control commands are issued as **broadcast frames** (slave address 0x88) with **custom function codes**. Standard FC06 only supports power-limit at register 0x9000.

### Standard FC 0x06 — Real-time active power limit

| Address | Name | FC | Type | Scale | Unit | Notes |
|---|---|---|---|---|---|---|
| 0x9000 | Active power limit (real-time) | 06 | U16 | 1 | % | 0x0064 = 100% (range 0–100) |

### Broadcast control (slave 0x88)

| Reg Addr | Name | FC | Value | Notes |
|---|---|---|---|---|
| 0x0142 | Remote ON/OFF | 0x01 | 0x0055 = ON, 0x0066 = OFF | Broadcast 0x88 only |
| 0x0141 | Active power derate setting | 0x01 | 0–100 (%) | Broadcast 0x88 |
| 0x0161 | Power factor setpoint | 0x01 | signed PF * 1000 | Broadcast 0x88 |
| 0x0162 | Reactive power setpoint | 0x01 | kVar value | Broadcast 0x88 |
| 0x5000 | Auto timing (clock sync) | 0x02 | sec/min/hr/day/mo/yr (BCD) | Broadcast 0x88 |

### Grid-tie remote control

Use broadcast 0x88 / FC 0x01 / addr 0x0142 with 0x0055 (ON) or 0x0066 (OFF).
There is **no per-slave addressed ON/OFF** in this protocol revision.

---

## Summary of gaps for 50KTL 4-MPPT mapping

1. **Device info (model/SN/firmware)**: NOT FOUND in PDF.
2. **MPPT3 / MPPT4 V/I/P** in inverter block (0x000C+): NOT FOUND. Use combiner block (0x0109/0x010A for MPPT2-A etc.) as workaround, or request a 50KTL-specific addendum.
3. **Power factor monitoring**: NOT FOUND (setpoint only).
4. **Per-slave control (FC06/FC16)**: NOT FOUND — all control is via broadcast 0x88 with custom FCs. RTU master must support sending broadcast frames; ACK is not returned.
5. **Active power limit via standard FC06**: supported at 0x9000 only.
