# Kstar / Huawei Error/Alarm Code Bit Field Definitions

Source: Kstar KSG1250K Modbus Protocol v35 PDF, Huawei SUN2000MC Modbus Interface Definitions PDF

---

## Kstar

### ErrorCode1 (DSP Error Code, addr 0x0BD4-0x0BD5 = reg 3028-3029, U32, Table 3.1.3)

```python
BITS = {
    0:  'GridVoltLow',
    1:  'GridVoltHigh',
    2:  'GridFrequencyLow',
    3:  'GridFrequencyHigh',
    4:  'BusVoltLow',
    5:  'BusVoltHigh',
    6:  'BusVoltUnbalance',
    7:  'IsolationFault',
    8:  'PVCurrentHigh',
    9:  'HardInverterCurrentOver',
    10: 'InverterCurrentOver',
    11: 'InverterDcCurrentOver',
    12: 'AmbientTemperatureOver',
    13: 'SinkTemperatureOver',
    14: 'ACRelayFault',
    15: 'Reserved',
    16: 'RemoteOff',
    17: 'Reserved',
    18: 'SPICommunicationFail',
    19: 'SPI2CommunicationFail',
    20: 'GFCIOverFault',
    21: 'GFCIDeviceFault',
    22: 'VoltageConsistentFault',
    23: 'FrequencyConsistentFault',
    24: 'Reserved',
    25: 'AuxiliaryPowerOff',
    26: 'IGBTFault',
    27: 'NPEVoltFault',
    28: 'DCOverVoltSeriousFault',
    29: 'IGBTSeriousFault',
    30: 'Reserved',
    31: 'Reserved',
}
```

Status: ALREADY POPULATED in Kstar_60_3_registers.py (bits 0-14,16,18-23,25-29).
Missing from current file: bit 15 (Reserved), 17 (Reserved), 24 (Reserved), 30-31 (Reserved) -- all reserved, no action needed.

### ErrorCode2 (DSP Alarm Code, addr 0x0BD3 = reg 3027, U16, Table 3.1.2)

```python
BITS = {
    0: 'FanALock',
    1: 'FanBLock',
    2: 'FanCLock',
    3: 'ZeroPower',
    4: 'ArrayWarning',
    5: 'Reserved',
    6: 'LightningWarning',
    7: 'PVParallelOpen',
}
```

Status: ALREADY POPULATED in Kstar_60_3_registers.py (bits 0-4, 6-7). Matches PDF exactly.

### ErrorCode3 (reserved -- no separate register)

```python
BITS = {}  # No ErrorCode3 register in Kstar protocol
```

Note: ARM alarm/error codes (reg 3036, Table 3.1.6/3.1.7) are U8 values (not bitfield mapped to ErrorCode3).
- ARM Alarm (Table 3.1.6): bit0=ClockWarning, bit1=Fan4Lock, bit2=Fan5Lock, bit3=Fan7Lock, bit4=Fan8Lock, bit5=LightingWarning, bit6=DSPVersionWarning, bit7=FuseWireWarning
- ARM Error (Table 3.1.7): bit0=DSPCommunicationError

---

## Huawei

### ErrorCode1 = ALARM1 (addr 0x7D08 = reg 32008, U16, Table 5-1)

```python
BITS = {
    0:  'HighStringInputVoltage',
    1:  'DCArcFault',
    2:  'StringReverseConnection',
    3:  'StringCurrentBackfeed',
    4:  'AbnormalStringPower',
    5:  'AFCISelfCheckFail',
    6:  'PhaseWireShortCircuitedToPE',
    7:  'GridLoss',
    8:  'GridUndervoltage',
    9:  'GridOvervoltage',
    10: 'GridVoltImbalance',
    11: 'GridOverfrequency',
    12: 'GridUnderfrequency',
    13: 'UnstableGridFrequency',
    14: 'OutputOvercurrent',
    15: 'OutputDCComponentOverhigh',
}
```

### ErrorCode2 = ALARM2 (addr 0x7D09 = reg 32009, U16, Table 5-1)

```python
BITS = {
    0:  'AbnormalResidualCurrent',
    1:  'AbnormalGrounding',
    2:  'LowInsulationResistance',
    3:  'Overtemperature',
    4:  'DeviceFault',
    5:  'UpgradeFailedOrVersionMismatch',
    6:  'LicenseExpired',
    7:  'FaultyMonitoringUnit',
    8:  'FaultyPowerCollector',
    9:  'BatteryAbnormal',
    10: 'ActiveIslanding',
    11: 'PassiveIslanding',
    12: 'TransientACOvervoltage',
    13: 'PeripheralPortShortCircuit',
    14: 'ChurnOutputOverload',
    15: 'AbnormalPVModuleConfiguration',
}
```

### ErrorCode3 = ALARM3 (addr 0x7D0A = reg 32010, U16, Table 5-1)

```python
BITS = {
    0: 'OptimizerFault',
    1: 'BuiltInPIDOperationAbnormal',
    2: 'HighInputStringVoltageToGround',
    3: 'ExternalFanAbnormal',
    4: 'BatteryReverseConnection',
    5: 'OnGridOffGridControllerAbnormal',
    6: 'PVStringLoss',
    7: 'InternalFanAbnormal',
    8: 'DCProtectionUnitAbnormal',
}
```

Status: ALL THREE EMPTY in Huawei_50_3_registers.py -- needs population.

### Additional registers (NOT mapped to ErrorCode):
- ALARM4 (0x7D0B = reg 32011): NOT FOUND in PDF Table 5-1
- ALARM5 (0x7D0C = reg 32012): NOT FOUND in PDF Table 5-1
- ALARM6 (0x7D0D = reg 32013): NOT FOUND in PDF Table 5-1

---

## Summary

| Inverter | Register     | Addr       | Current Status           |
|----------|------------- |------------|--------------------------|
| Kstar    | ErrorCode1   | 0x0BD4 U32 | Already populated        |
| Kstar    | ErrorCode2   | 0x0BD3 U16 | Already populated        |
| Kstar    | ErrorCode3   | N/A        | Empty (correct, no reg)  |
| Huawei   | ErrorCode1   | 0x7D08 U16 | EMPTY -- needs 16 bits   |
| Huawei   | ErrorCode2   | 0x7D09 U16 | EMPTY -- needs 16 bits   |
| Huawei   | ErrorCode3   | 0x7D0A U16 | EMPTY -- needs 9 bits    |
