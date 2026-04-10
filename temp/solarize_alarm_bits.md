# Solarize Inverter Error/Alarm Code Bit Fields
# Source: Solarize-PV_Modbus_Protocol-Korea-V1.2.4.pdf (Appendix)

```python
# ErrorCode1 (0x101E) - 16 bits
ERROR_CODE_1_BITS = {
    0:  "INVERTER_OVER_DC_BIAS_CURRENT",
    1:  "INVERTER_RELAY_ABNORMAL",
    2:  "REMOTE_OFF",
    3:  "INVERTER_OVER_TEMPERATURE",
    4:  "GFCI_ABNORMAL",
    5:  "PV_STRING_REVERSE",
    6:  "SYSTEM_TYPE_ERROR",
    7:  "FAN_ABNORMAL",
    8:  "DC_LINK_UNBALANCE_OR_UNDER_VOLTAGE",
    9:  "DC_LINK_OVER_VOLTAGE",
    10: "INTERNAL_COMMUNICATION_ERROR",
    11: "SOFTWARE_INCOMPATIBILITY",
    12: "INTERNAL_STORAGE_ERROR",
    13: "DATA_INCONSISTENCY",
    14: "INVERTER_ABNORMAL",
    15: "BOOST_ABNORMAL",
}

# ErrorCode2 (0x101F) - 16 bits
ERROR_CODE_2_BITS = {
    0:  "GRID_OVER_VOLTAGE",
    1:  "GRID_UNDER_VOLTAGE",
    2:  "GRID_ABSENT",
    3:  "GRID_OVER_FREQUENCY",
    4:  "GRID_UNDER_FREQUENCY",
    5:  "PV_OVER_VOLTAGE",
    6:  "PV_INSULATION_ABNORMAL",
    7:  "LEAKAGE_CURRENT_ABNORMAL",
    8:  "INVERTER_IN_POWER_LIMIT_STATE",
    9:  "INTERNAL_POWER_SUPPLY_ABNORMAL",
    10: "PV_STRING_ABNORMAL",
    11: "PV_UNDER_VOLTAGE",
    12: "PV_IRRADIATION_WEAK",
    13: "GRID_ABNORMAL",
    14: "ARC_FAULT_DETECTION",
    15: "AC_MOVING_AVERAGE_VOLTAGE_HIGH",
}

# ErrorCode3 (0x1020) - 16 bits (bit 0, 2, 15 are Reserved -> omitted)
ERROR_CODE_3_BITS = {
    1:  "LOGGER_EDISPLAY_EEPROM_FAIL",
    3:  "SINGLE_TRACKER_DETECT_WARNING_PID_ABNORMAL",
    4:  "AFCI_LOST",
    5:  "DATA_LOGGER_LOST",
    6:  "METER_LOST",
    7:  "INVERTER_LOST",
    8:  "GRID_N_ABNORMAL",
    9:  "SPD_DEFECTIVE",
    10: "PARALLEL_ID_WARNING",
    11: "PARALLEL_SYN_SIGNAL_WARNING",
    12: "PARALLEL_BAT_ABNORMAL",
    13: "PARALLEL_GRID_ABNORMAL",
    14: "GENERATOR_VOLTAGE_ABNORMAL",
}

# REMS Error Code Table (0x7E response, 2 bytes)
REMS_ERROR_CODE_BITS = {
    0:  "INVERTER_STOP",            # 0=running, 1=stop
    1:  "SOLAR_BATTERY_OVER_VOLTAGE",
    2:  "SOLAR_BATTERY_UNDER_VOLTAGE",
    3:  "SOLAR_BATTERY_OVER_CURRENT",
    4:  "INVERTER_IGBT_ERROR",
    5:  "INVERTER_OVER_TEMPERATURE",
    6:  "GRID_OVER_VOLTAGE",
    7:  "GRID_UNDER_VOLTAGE",
    8:  "GRID_OVER_CURRENT",
    9:  "GRID_OVER_FREQUENCY",
    10: "GRID_UNDER_FREQUENCY",
    11: "SELF_CONTROL_OUTAGE",
    12: "LEAKAGE_CURRENT_ABNORMAL",
}

# Inverter Mode Table (0x101D) - for reference
INVERTER_MODE = {
    0x00: "INITIAL",
    0x01: "STANDBY",
    0x03: "ON_GRID",
    0x04: "OFF_GRID",
    0x05: "FAULT",
    0x09: "SHUTDOWN",
}

# History Error Code format (0xB600-B601):
# 15-bit error number field -> maps to sequential "No" column:
# ErrorCode1 bits = No 16-31, ErrorCode2 bits = No 0-15, ErrorCode3 bits = No 32-47
# Bit 15 of error word: 0=warning set, 1=warning clear
```

## Register Address Summary

| Register | Name | Type |
|----------|------|------|
| 0x101D | Inverter Mode | U16 (mode enum) |
| 0x101E | Error Code 1 | U16 (bit field) |
| 0x101F | Error Code 2 | U16 (bit field) |
| 0x1020 | Error Code 3 | U16 (bit field) |

Note: 0x0009/0x000A registers are not defined in this PDF.
The Solarize protocol uses 0x1000-range addresses for real-time data.
