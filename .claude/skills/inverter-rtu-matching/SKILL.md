---
name: inverter-rtu-matching
description: "Inverter Modbus PDF -> RTU Register File Generator. PDF parsing -> register extraction -> UDP H01 field matching -> _registers.py code generation. API not required."
argument-hint: "[PDF file path] [MPPT count] [String count]"
disable-model-invocation: true
allowed-tools: Bash Read Write Edit Grep Glob Agent
---

# Inverter RTU Matching - PDF to Register File

Inverter Modbus Protocol PDF -> RTU compatible `_registers.py` file generation.
Claude directly parses the PDF and generates the register file without external AI API.

## Overview

Solar inverter Modbus protocol PDF document -> extract registers -> match to UDP H01 body fields ->
generate `_registers.py` Python file that enables RTU to:
1. Read inverter data via Modbus RTU RS485
2. Pack data into UDP H01 packets and send to monitoring server
3. Receive H03 control commands from server and write to inverter

## Execution Steps

### Step 1: Read and parse the PDF

```python
# Use the project's PDF parser
import sys; sys.path.insert(0, 'inverter_model_maker/model_maker_web_v2/backend')
from pipeline.stage1 import extract_pdf_text_and_tables
pages = extract_pdf_text_and_tables(pdf_path)
# pages[i]['text'], pages[i]['tables']
```

Or read PDF directly with `fitz` (PyMuPDF) and identify register tables.

### Step 2: Extract registers from PDF

Extract ONLY these categories of registers (target: 40-80 unique addresses):

#### MONITORING registers (most important)
| Standard Name | Description | Typical Scale |
|--------------|-------------|---------------|
| L1_VOLTAGE, L2_VOLTAGE, L3_VOLTAGE | AC phase-to-neutral voltages | 0.1V |
| L1_CURRENT, L2_CURRENT, L3_CURRENT | AC phase currents | 0.01A |
| L2_FREQUENCY, L3_FREQUENCY | Per-phase frequency (if exists) | 0.01Hz |
| FREQUENCY | Grid frequency (primary) | 0.01Hz |
| PHASE_A_POWER, PHASE_B_POWER, PHASE_C_POWER | Per-phase AC power (if exists) | varies |
| ACTIVE_POWER | Total AC active power (W) | varies |
| REACTIVE_POWER | Total reactive power (Var) | varies |
| POWER_FACTOR | Grid power factor | 0.001 |
| PV_POWER | Total DC input power | varies |
| PV{N}_VOLTAGE, PV{N}_CURRENT | Per-MPPT DC input (N=1,2,3...) | 0.1V/0.01A |
| MPPT{N}_POWER | Per-MPPT power (if exists) | varies |
| STRING{N}_CURRENT | Per-string current (if separate from MPPT) | 0.01A |
| CUMULATIVE_ENERGY | Lifetime total energy (kWh) | varies |
| DAILY_ENERGY | Today's energy | varies |
| INNER_TEMP | Inverter internal temperature | 0.1C |
| L1_WATT_OF_GRID, L2_WATT_OF_GRID, L3_WATT_OF_GRID | Grid meter power (if CT present) | varies |
| L1_CURRENT_OF_GRID, L2_CURRENT_OF_GRID, L3_CURRENT_OF_GRID | Grid meter current | varies |
| L1_N_PHASE_VOLTAGE_OF_GRID/LOAD | Grid/Load voltages | varies |
| ACCUMULATED_ENERGY_OFIMPORT, OFEXPORT, OF_LOAD | Energy metering | varies |

#### STATUS register
| Standard Name | Description |
|--------------|-------------|
| INVERTER_MODE | Operating state - extract ALL mode values from PDF |

Mode values must be normalized to:
- INITIAL (0x00 etc.) - boot/initialization
- STANDBY (0x01 etc.) - waiting/idle
- ON_GRID (0x03 etc.) - normal/running/generating
- FAULT (0x05 etc.) - fault/error/abnormal
- SHUTDOWN (0x09 etc.) - off/stopped
- STARTUP - starting (optional)

#### ALARM registers (1-3 registers)
| Standard Name | Description |
|--------------|-------------|
| ERROR_CODE1, ERROR_CODE2, ERROR_CODE3 | Fault/alarm registers |

For each alarm register, determine if it's:
- **Bitfield**: Each bit = different fault -> extract ALL bits from PDF appendix
  ```python
  BITS = {0: 'OVER_TEMPERATURE', 1: 'GROUND_FAULT', 2: 'DC_OVERVOLTAGE', ...}
  ```
- **Enum**: Register value = fault code number -> extract ALL codes
  ```python
  FAULT_CODE_TABLE = {1: 'Grid overvoltage', 2: 'Grid undervoltage', ...}
  ```

#### CONTROL registers (3-5, writable RW)
| Standard Name | H03 Control Type | Value Range |
|--------------|-----------------|-------------|
| INVERTER_ON_OFF | INV_ON_OFF (15) | 0=off, 1=on |
| ACTIVE_POWER_LIMIT | INV_ACTIVE_POWER (16) | 0-1000 (0-100.0%) |
| POWER_FACTOR_SET | INV_POWER_FACTOR (17) | -1000~1000 (-1.0~1.0) |
| REACTIVE_POWER_PCT | INV_REACTIVE_POWER (18) | percentage |

#### DEVICE_INFO registers
| Standard Name | Type | Description |
|--------------|------|-------------|
| DEVICE_MODEL or DEVICE_MODEL_NAME | STRING | Model name |
| DEVICE_SERIAL_NUMBER | STRING | Serial number |
| FIRMWARE_VERSION or MASTER_FIRMWARE_VERSION | STRING | Main FW version |
| SLAVE_FIRMWARE_VERSION | STRING | Secondary FW (if exists) |
| NOMINAL_VOLTAGE, NOMINAL_FREQUENCY | U16 | Rated values |
| GRID_PHASE_NUMBER | U16 | Number of phases |

### DO NOT extract:
- Communication settings (baud rate, slave address, protocol version)
- Time/date/clock registers
- Reserved/unused registers
- Grid protection limit settings (OV/UV thresholds) unless writable control
- Statistics/counters (run hours, boot count)
- Duplicate representations of same value

### Step 3: Determine MPPT/String configuration

From PDF or user input:
- **MPPT count**: Number of MPPT channels (PV1..PVN)
- **Total strings**: Total string count across all MPPTs
- **Strings per MPPT**: total_strings / mppt_count (integer)

### Step 4: Generate the `_registers.py` file

Output file must contain ALL of these elements:

#### 4.1 class RegisterMap
```python
class RegisterMap:
    # === INFO ===
    DEVICE_MODEL_NAME                        = 0x1A00  # STRING
    DEVICE_SERIAL_NUMBER                     = 0x1A10  # STRING
    FIRMWARE_VERSION                         = 0x1A1C  # STRING

    # === MONITORING ===
    L1_VOLTAGE                               = 0x1002  # U16, 0.1V
    L2_VOLTAGE                               = 0x1003  # ...
    # ... all monitoring registers ...

    # === STATUS ===
    INVERTER_MODE                            = 0x101D  # U16

    # === ALARM ===
    ERROR_CODE1                              = 0x101E  # U16
    ERROR_CODE2                              = 0x101F  # U16
    ERROR_CODE3                              = 0x1020  # U16

    # === U32 _HIGH words (auto) ===
    ACTIVE_POWER_HIGH                        = 0x100B  # U32 high word
    # (for every U32/S32 register, add _HIGH = addr+1)

    # === MPPT aliases ===
    MPPT1_VOLTAGE                            = PV1_VOLTAGE
    MPPT1_CURRENT                            = PV1_CURRENT
    # ... for each MPPT ...

    # === R/S/T Phase aliases ===
    R_VOLTAGE                                = L1_VOLTAGE
    R_CURRENT                                = L1_CURRENT
    S_VOLTAGE                                = L2_VOLTAGE
    # ... R=L1, S=L2, T=L3 ...

    # === Power aliases ===
    AC_POWER                                 = ACTIVE_POWER
    AC_POWER_HIGH                            = ACTIVE_POWER_HIGH
    PV_POWER                                 = PV_TOTAL_INPUT_POWER  # (or compute)
    TOTAL_ENERGY                             = CUMULATIVE_ENERGY

    # === PV_STRING_COUNT ===
    PV_STRING_COUNT                          = 8  # total strings

    # === STRING voltage aliases (fallback to MPPT voltage) ===
    STRING1_VOLTAGE                          = MPPT1_VOLTAGE
    STRING2_VOLTAGE                          = MPPT1_VOLTAGE
    STRING3_VOLTAGE                          = MPPT2_VOLTAGE
    # ... ceil(N/strings_per_mppt) -> MPPT index ...

    # === STRING current aliases (if separate registers exist) ===
    STRING1_CURRENT                          = 0x1051  # real address
    # OR alias: STRING1_CURRENT              = PV1_CURRENT

    # === ERROR_CODE aliases ===
    ERROR_CODE1                              = 0x101E  # (or alias)

    # === DER-AVM Monitor (fixed addresses, ALWAYS include) ===
    DEA_L1_CURRENT_LOW                       = 0x03E8
    DEA_L1_CURRENT_HIGH                      = 0x03E9
    DEA_L2_CURRENT_LOW                       = 0x03EA
    DEA_L2_CURRENT_HIGH                      = 0x03EB
    DEA_L3_CURRENT_LOW                       = 0x03EC
    DEA_L3_CURRENT_HIGH                      = 0x03ED
    DEA_L1_VOLTAGE_LOW                       = 0x03EE
    DEA_L1_VOLTAGE_HIGH                      = 0x03EF
    DEA_L2_VOLTAGE_LOW                       = 0x03F0
    DEA_L2_VOLTAGE_HIGH                      = 0x03F1
    DEA_L3_VOLTAGE_LOW                       = 0x03F2
    DEA_L3_VOLTAGE_HIGH                      = 0x03F3
    DEA_TOTAL_ACTIVE_POWER_LOW               = 0x03F4
    DEA_TOTAL_ACTIVE_POWER_HIGH              = 0x03F5
    DEA_TOTAL_REACTIVE_POWER_LOW             = 0x03F6
    DEA_TOTAL_REACTIVE_POWER_HIGH            = 0x03F7
    DEA_POWER_FACTOR_LOW                     = 0x03F8
    DEA_POWER_FACTOR_HIGH                    = 0x03F9
    DEA_FREQUENCY_LOW                        = 0x03FA
    DEA_FREQUENCY_HIGH                       = 0x03FB
    DEA_STATUS_FLAG_LOW                      = 0x03FC
    DEA_STATUS_FLAG_HIGH                     = 0x03FD

    # === DER-AVM Control ===
    DER_POWER_FACTOR_SET                     = 0x07D0
    DER_ACTION_MODE                          = 0x07D1
    DER_REACTIVE_POWER_PCT                   = 0x07D2
    DER_ACTIVE_POWER_PCT                     = 0x07D3
    INVERTER_ON_OFF                          = 0x0834  # (actual addr from PDF)
    DER_AVM_DIGITAL_METERCONNECT_STATUS      = 0x1210
```

#### 4.2 class InverterMode
```python
class InverterMode:
    INITIAL   = 0x00
    STANDBY   = 0x01
    ON_GRID   = 0x03
    FAULT     = 0x05
    SHUTDOWN  = 0x09
    # ... values from PDF ...

    @classmethod
    def to_string(cls, status):
        _map = {0x00: 'Initial', 0x01: 'Standby', ...}
        return _map.get(status, f'Unknown(0x{status:04X})')
```

#### 4.3 ErrorCode classes (per alarm register)
```python
class ErrorCode1:
    BITS = {
        0: 'OVER_TEMPERATURE',
        1: 'GROUND_FAULT',
        # ... ALL bits from PDF appendix ...
    }
    @classmethod
    def to_string(cls, code):
        # bitfield: return list of active faults
        # enum: return single fault name
```

#### 4.4 SCALE dict (MANDATORY keys)
```python
SCALE = {
    'voltage':            0.1,    # from PDF (mandatory)
    'current':            0.01,   # from PDF (mandatory)
    'power':              0.1,    # from PDF (mandatory)
    'frequency':          0.01,   # from PDF (mandatory)
    'power_factor':       0.001,  # from PDF (mandatory)
    'dea_current':        0.1,    # DER-AVM (fixed)
    'dea_voltage':        0.1,    # DER-AVM (fixed)
    'dea_active_power':   0.1,    # DER-AVM (fixed)
    'dea_reactive_power': 1,      # DER-AVM (fixed)
    'dea_frequency':      0.1,    # DER-AVM (fixed)
    'iv_voltage':         0.1,    # IV Scan (fixed)
    'iv_current':         0.1,    # IV Scan (fixed)
}
```

#### 4.5 Helper functions
```python
def registers_to_u32(high, low):
    return ((high & 0xFFFF) << 16) | (low & 0xFFFF)

def registers_to_s32(high, low):
    val = registers_to_u32(high, low)
    return val - 0x100000000 if val >= 0x80000000 else val
```

#### 4.6 H01_FIELD_MAP (16 scalar fields -> RegisterMap attr + converter)

This maps UDP H01 body fields to register attributes:
```python
H01_FIELD_MAP = {
    'mode':              ('INVERTER_MODE',     'raw'),
    'r_voltage':         ('L1_VOLTAGE',        'voltage_to_V'),
    's_voltage':         ('L2_VOLTAGE',        'voltage_to_V'),
    't_voltage':         ('L3_VOLTAGE',        'voltage_to_V'),
    'r_current':         ('L1_CURRENT',        'current_to_01A'),
    's_current':         ('L2_CURRENT',        'current_to_01A'),
    't_current':         ('L3_CURRENT',        'current_to_01A'),
    'frequency':         ('FREQUENCY',         'frequency_to_01Hz'),
    'ac_power':          ('AC_POWER',           'power_to_W'),
    'pv_power':          ('PV_POWER',           'power_to_W'),
    'inner_temp':        ('INNER_TEMP',         'raw'),
    'power_factor':      ('POWER_FACTOR',       'pf_raw'),
    'cumulative_energy': ('CUMULATIVE_ENERGY',  'energy_kwh_to_Wh'),
    'alarm1':            ('ERROR_CODE1',        'raw'),
    'alarm2':            ('ERROR_CODE2',        'raw'),
    'alarm3':            ('ERROR_CODE3',        'raw'),
}
```

Converter keys:
- `voltage_to_V`: raw * SCALE['voltage'] -> volts, then * 10 for H01 (0.1V units)
- `current_to_01A`: raw * SCALE['current'] -> amps, then * 100 for H01 (0.01A units)
- `frequency_to_01Hz`: raw * SCALE['frequency'] -> Hz, then * 100 for H01 (0.01Hz units)
- `power_to_W`: raw * SCALE['power'] -> watts (U32 for H01)
- `energy_kwh_to_Wh`: raw * SCALE -> kWh, then * 1000 for H01 (Wh, U64)
- `pf_raw`: raw * SCALE['power_factor'] -> -1.0~1.0, then * 1000 for H01 (S16)
- `raw`: no conversion

#### 4.7 Dynamic fields (MPPT + String)
```python
# Per-MPPT (added dynamically to H01_FIELD_MAP)
'mppt1_voltage': ('MPPT1_VOLTAGE', 'voltage_to_V'),
'mppt1_current': ('MPPT1_CURRENT', 'current_to_01A'),
# ... for N MPPTs ...

# Per-String (added dynamically)
'string1_voltage': ('STRING1_VOLTAGE', 'voltage_to_V'),
'string1_current': ('STRING1_CURRENT', 'current_to_01A'),
# ... for N strings ...
```

#### 4.8 READ_BLOCKS (Modbus read optimization)
```python
READ_BLOCKS = [
    {'start': 0x1001, 'count': 96, 'fc': 3},  # main monitoring block
    {'start': 0x1A00, 'count': 32, 'fc': 3},  # device info block
    # Group contiguous registers, max 125 per block
    # Separate by FC (3=Holding, 4=Input) if mixed
]
```

#### 4.9 DATA_PARSER (register addr -> H01 field mapping for RTU)
```python
DATA_PARSER = {
    'mode':              'INVERTER_MODE',
    'r_voltage':         'L1_VOLTAGE',
    's_voltage':         'L2_VOLTAGE',
    't_voltage':         'L3_VOLTAGE',
    'r_current':         'L1_CURRENT',
    's_current':         'L2_CURRENT',
    't_current':         'L3_CURRENT',
    'frequency':         'FREQUENCY',
    'ac_power':          'AC_POWER',
    'cumulative_energy': 'CUMULATIVE_ENERGY',
    'alarm1':            'ERROR_CODE1',
    'mppt1_voltage':     'MPPT1_VOLTAGE',
    'mppt1_current':     'MPPT1_CURRENT',
    # ... per MPPT ...
    'string1_voltage':   'STRING1_VOLTAGE',
    'string1_current':   'STRING1_CURRENT',
    # ... per String ...
}
```

#### 4.10 Additional required elements
```python
class IVScanCommand:     # IV curve scan (if inverter supports)
class IVScanStatus:
class DerActionMode:     # DER-AVM action modes (ALWAYS include)
    POWER_FACTOR = 1
    DER_AVM_CONTROL = 2
    REACTIVE_POWER = 3
    ACTIVE_POWER = 4

class DeviceType:
    PV = "PV"
    HYB = "HYB"
    DER_AVM = "DER_AVM"

# StatusConverter class
class StatusConverter:
    @staticmethod
    def to_inverter_mode(raw_value):
        return InverterMode.to_string(raw_value)

# DATA_TYPES dict (register name -> type string)
DATA_TYPES = {
    'INVERTER_MODE': 'U16',
    'L1_VOLTAGE': 'U16',
    'ACTIVE_POWER': 'U32',
    # ... for all registers ...
}

# MPPT_CHANNELS, STRING_CHANNELS
MPPT_CHANNELS = 4
STRING_CHANNELS = 8

# get_mppt_registers(), get_string_registers()
def get_mppt_registers(mppt_num):
    return {'voltage': f'MPPT{mppt_num}_VOLTAGE', 'current': f'MPPT{mppt_num}_CURRENT'}

def get_string_registers(string_num):
    return {'voltage': f'STRING{string_num}_VOLTAGE', 'current': f'STRING{string_num}_CURRENT'}
```

## UDP Protocol Data Flow

```
Inverter <--Modbus RTU RS485--> RTU <--UDP--> Server <--WebSocket--> Dashboard

[Monitoring Flow - every 60 seconds]
1. RTU reads registers per READ_BLOCKS (FC03/FC04 Modbus requests)
2. Raw values converted via SCALE dict and H01_FIELD_MAP converters
3. Packed into H01 binary packet (20-byte header + 44-byte body + MPPT array + String array)
4. Sent to server via UDP
5. Server parses H01, stores in SQLite, broadcasts via WebSocket
6. Dashboard displays real-time data

[Control Flow - on demand]
1. Dashboard user sends control command (on/off, power limit, PF)
2. Server sends H03 packet to RTU (control_type + value)
3. RTU maps H03 control_type to register address:
   - INV_ON_OFF (15)       -> RegisterMap.INVERTER_ON_OFF (FC06)
   - INV_ACTIVE_POWER (16) -> RegisterMap.ACTIVE_POWER_LIMIT (FC06)
   - INV_POWER_FACTOR (17) -> RegisterMap.POWER_FACTOR_SET (FC06)
   - INV_REACTIVE_POWER(18)-> RegisterMap.REACTIVE_POWER_PCT (FC06)
4. RTU writes to inverter via Modbus FC06 (single) or FC16 (multiple)
5. RTU sends H04 response back to server with result code
```

## H01 Packet Binary Format

### Header (20 bytes)
```
Offset  Size  Field         Type    Value
0       1     Version       uint8   0x01
1       2     Sequence      uint16  auto-increment
3       4     RTU_ID        uint32  from config
7       8     Timestamp     uint64  unix ms
15      1     DeviceType    uint8   1=Inverter
16      1     DeviceNumber  uint8   inverter index
17      1     Model         uint8   inverter model enum
18      1     BackupFlag    uint8   0=live, 1=backup
19      1     BodyType      int8    4=MPPT+STRING
```

### Inverter Body (44 bytes base)
```
Offset  Size  Field              Type   H01_FIELD_MAP key
0       2     pv_voltage         U16    (from mppt1_voltage)
2       2     pv_current         U16    (from mppt1_current)
4       4     pv_power           U32    pv_power
8       2     r_voltage          U16    r_voltage
10      2     s_voltage          U16    s_voltage
12      2     t_voltage          U16    t_voltage
14      2     r_current          U16    r_current
16      2     s_current          U16    s_current
18      2     t_current          U16    t_current
20      4     ac_power           U32    ac_power
24      2     power_factor       S16    power_factor
26      2     frequency          U16    frequency
28      8     cumulative_energy  U64    cumulative_energy
36      2     status             U16    mode (InverterMode)
38      2     alarm1             U16    alarm1 (ErrorCode1)
40      2     alarm2             U16    alarm2 (ErrorCode2)
42      2     alarm3             U16    alarm3 (ErrorCode3)
```

### MPPT Array (variable)
```
44      1     mppt_count         uint8  N
45+i*4  2     mppt[i]_voltage    U16    (0.1V units)
47+i*4  2     mppt[i]_current    U16    (0.1A units)
```

### String Array (variable, after MPPT)
```
+0      1     string_count       uint8  M
+1+j*2  2     string[j]_current  U16    (0.1A units)
```

## DER-AVM Control Protocol

RTU exposes inverter data to DER-AVM master via Modbus RTU slave on RS485 CH2.

### DER Monitor registers (FC03 read, fixed addresses)
11 S32 pairs at 0x03E8~0x03FD: L1-L3 current, L1-L3 voltage, active/reactive power, PF, frequency, status flag.

### DER Control registers (FC06/FC16 write)
- 0x07D0: DER_POWER_FACTOR_SET
- 0x07D1: DER_ACTION_MODE (1=PF, 2=DER_AVM, 3=Q%, 4=P%)
- 0x07D2: DER_REACTIVE_POWER_PCT
- 0x07D3: DER_ACTIVE_POWER_PCT
- 0x0834: INVERTER_ON_OFF

### Broadcast (0x00)
DER-AVM master can send FC06/FC16 to slave address 0x00 for group control.
All inverters execute the command, no response is sent.

## Output File Location

Generated file should be saved to:
```
inverter_model_maker/common/{Protocol}_{Capacity}_{Phase}_{MPPT}_{String}_registers.py
```

Example: `Solarize_50_3_MPPT4_STR8_registers.py`
- Protocol: manufacturer/protocol name
- Capacity: kW rating (e.g., 50)
- Phase: 3 (three-phase), 1 (single-phase)
- MPPT/STR: channel counts

Then copy to `common/` for RTU deployment:
```bash
cp inverter_model_maker/common/NewBrand_50_3_registers.py common/
```

## Validation Checklist

After generating the file, verify:
- [ ] `class RegisterMap` has all required registers
- [ ] All U32/S32 registers have `_HIGH` counterpart at addr+1
- [ ] `class InverterMode` has at least INITIAL, STANDBY, ON_GRID, FAULT, SHUTDOWN
- [ ] ErrorCode classes have BITS dict (or FAULT_CODE_TABLE for enum types)
- [ ] `SCALE` dict has all 5 mandatory keys + 7 DER/IV keys
- [ ] `H01_FIELD_MAP` has all 16 fields with correct converter keys
- [ ] `DATA_PARSER` maps all H01 fields + MPPT + String fields
- [ ] `READ_BLOCKS` covers all monitoring/status/alarm registers
- [ ] `get_mppt_registers()` returns correct register names for each MPPT
- [ ] `get_string_registers()` returns correct register names for each String
- [ ] DEA_* registers are at fixed addresses 0x03E8~0x03FD
- [ ] DER control registers at 0x07D0~0x07D3 + INVERTER_ON_OFF
- [ ] File can be imported without syntax errors: `python -c "import {filename}"`
