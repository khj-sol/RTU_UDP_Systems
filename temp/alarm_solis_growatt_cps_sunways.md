# Inverter Error/Alarm Code Extraction Report

Source: PDF Modbus protocol manuals
Date: 2026-04-09

---

## 1. Solis (Solis_50_3_registers.py)

Registers: FAULT_CODE_01~05 at 3096~3100 (U16 bitfield each)

### ErrorCode1 (reg 3096) - BITS
| Bit | Name |
|-----|------|
| 0 | GRID_AB_OVERVOLTAGE |
| 1 | GRID_BC_OVERVOLTAGE |
| 2 | GRID_CA_OVERVOLTAGE |
| 3 | GRID_AB_UNDERVOLTAGE |
| 4 | GRID_BC_UNDERVOLTAGE |
| 5 | GRID_CA_UNDERVOLTAGE |
| 6 | GRID_OVER_FREQUENCY |
| 7 | GRID_UNDER_FREQUENCY |
| 8 | GRID_UNBALANCE |
| 9 | RESERVED |
| 10 | GRID_FREQ_FLUCTUATION |
| 11 | GRID_PHASE_ABNORMAL |
| 12 | NO_GRID |
| 13 | GRID_REVERSE |
| 14 | GRID_AB_TRANSIENT_OV |
| 15 | HW_OVERCURRENT_SCREEN_FAIL |

### ErrorCode2 (reg 3097) - BITS
| Bit | Name |
|-----|------|
| 0 | DC_OVER_VOLTAGE_01 |
| 1 | DC_OVER_VOLTAGE_02 |
| 2 | DC_REVERSE |
| 3 | BUS_V_DETECT_INCONSISTENT |
| 4 | DC_BUS_OVER_VOLTAGE |
| 5 | DC_BUS_UNDER_VOLTAGE |
| 6 | DC_BUS_UNBALANCE |
| 7 | DC_BUS_V_DETECT_ABNORMAL |
| 8 | GRID_AB_RMS_AVG_OV |
| 9 | GRID_BC_RMS_AVG_OV |
| 10 | GRID_CA_RMS_AVG_OV |
| 11 | PV_MIDPOINT_GROUNDING |
| 12 | DC_BOOST_FAULT |
| 13 | DC_HW_OVERCURRENT |
| 14 | GRID_CURRENT_TRACKING_FAULT |
| 15 | GRID_V_RMS_INSTANT_OV |

### ErrorCode3 (reg 3098) - BITS
| Bit | Name |
|-----|------|
| 0 | PHASE_A_RMS_OVERCURRENT |
| 1 | PHASE_B_RMS_OVERCURRENT |
| 2 | PHASE_C_RMS_OVERCURRENT |
| 3 | DC1_AVG_OVERCURRENT |
| 4 | DC2_AVG_OVERCURRENT |
| 5 | AC_HW_OVERCURRENT |
| 6 | DC_COMPONENT_OVER_LIMIT |
| 7 | GRID_AB_OVERVOLTAGE_02 |
| 8 | GRID_BC_OVERVOLTAGE_02 |
| 9 | GRID_CA_OVERVOLTAGE_02 |
| 10 | GRID_AB_UNDERVOLTAGE_02 |
| 11 | GRID_BC_UNDERVOLTAGE_02 |
| 12 | GRID_CA_UNDERVOLTAGE_02 |
| 13 | GRID_OVER_FREQ_02 |
| 14 | GRID_UNDER_FREQ_02 |
| 15 | GRID_OV_03_LEVEL3 |

Note: Solis also has reg 3099 (Fault Code 4) and 3100 (Fault Code 5) with additional bits.
These are not mapped in current ErrorCode1/2/3 but can be added if needed.

---

## 2. Growatt (Growatt_30_3_registers.py)

### ErrorCode1 = FAULT_CODE (reg 0x0028=40, U32 bitfield, &*8)
### ErrorCode2 = WARNING_CODE (reg 0x0040=64, U16 bitfield, &*8)
### ErrorCode3 = FAULT_CODE_HL (reg 0x0080=128, U32 same as ErrorCode1 hi word)

### ErrorCode1 (reg 0x0028, U32) - BITS (fault code)
| Bit | Name |
|-----|------|
| 1 | COMM_ERROR |
| 3 | STR_REVERSE_OR_SHORT |
| 4 | MODEL_INIT_FAULT |
| 5 | GRID_VOLT_SAMPLE_DIFF |
| 6 | ISO_SAMPLE_DIFF |
| 7 | GFCI_SAMPLE_DIFF |
| 12 | AFCI_FAULT |
| 14 | AFCI_MODULE_FAULT |
| 17 | RELAY_CHECK_FAULT |
| 21 | COMM_ERROR_2 |
| 22 | BUS_VOLTAGE_ERROR |
| 23 | AUTO_TEST_FAIL |
| 24 | NO_UTILITY |
| 25 | PV_ISOLATION_LOW |
| 26 | RESIDUAL_I_HIGH |
| 27 | OUTPUT_HIGH_DCI |
| 28 | PV_VOLTAGE_HIGH |
| 29 | AC_V_OUTRANGE |
| 30 | AC_F_OUTRANGE |
| 31 | TEMPERATURE_HIGH |

Note: bits 0,2,8-11,13,15,16,18-20 marked "\" (not used) in PDF.

### ErrorCode2 (reg 0x0040, U16) - BITS (warning code)
| Bit | Name |
|-----|------|
| 0 | FAN_WARNING |
| 1 | STRING_COMM_ABNORMAL |
| 2 | STR_PID_CONFIG_WARNING |
| 3 | EEPROM_READ_FAIL |
| 4 | DSP_COM_FW_UNMATCH |
| 5 | EEPROM_WRITE_FAIL |
| 6 | SPD_ABNORMAL |
| 7 | GND_N_CONNECT_ABNORMAL |
| 8 | PV1_OR_PV2_SHORT |
| 9 | PV1_OR_PV2_BOOST_BROKEN |

Note: bits 10-15 marked "\" (not used) in PDF.

### ErrorCode3 (reg 0x0080)
Same fault code as ErrorCode1 (U32, high word at reg 0x0080).
Use same BITS as ErrorCode1.

---

## 3. CPS (CPS_50_3_registers.py)

Register: ERROR_CODE1 at 0x101E (U32 bitfield)
ErrorCode2/3 are aliases of ErrorCode1 in current code.

### ErrorCode1 (reg 0x101E, U32) - BITS
| Bit | Name |
|-----|------|
| 0 | GRID_AC_OVER_VOLTAGE |
| 1 | GRID_AC_UNDER_VOLTAGE |
| 2 | GRID_AC_ABSENT |
| 3 | GRID_AC_OVER_FREQUENCY |
| 4 | GRID_AC_UNDER_FREQUENCY |
| 5 | PV_DC_OVER_VOLTAGE |
| 6 | PV_INSULATION_ABNORMAL |
| 7 | LEAKAGE_CURRENT_ABNORMAL |
| 8 | GRID_AC_V_HIGHER_THAN_BUS |
| 9 | CONTROL_POWER_LOW |
| 10 | PV_STRING_ABNORMAL |
| 11 | PV_DC_UNDER_VOLTAGE |
| 12 | PV_IRRADIATION_WEAK |
| 13 | GRID_TYPE_UNKNOWN |
| 14 | ARC_FAULT_DETECTION |
| 15 | GROUND_CURRENT_300MA |
| 16 | OUTPUT_DC_OVER_CURRENT |
| 17 | INVERTER_RELAY_ABNORMAL |
| 18 | OUTPUT_DC_SENSOR_FAILED |
| 19 | INVERTER_OVER_TEMPERATURE |
| 20 | LEAKAGE_CURRENT_HCT_ABNORMAL |
| 21 | PV_STRING_REVERSE |
| 22 | SYSTEM_TYPE_ERROR |
| 23 | FAN_LOCK |
| 24 | BUS_UNDER_VOLTAGE |
| 25 | BUS_OVER_VOLTAGE |
| 26 | INTERNAL_COMM_ERROR |
| 27 | SOFTWARE_INCOMPATIBILITY |
| 28 | EEPROM_ERROR |
| 29 | CONSISTENT_WARNING |
| 30 | INVERTER_ABNORMAL |
| 31 | BOOST_ABNORMAL |

ErrorCode2/3: same register (aliases), no separate codes.

---

## 4. Sunways (Sunways_30_3_registers.py)

Registers: FAULT_FLAG1 at 10112 (U32), FAULT_FLAG2 at 10114 (U32), FAULT_FLAG3 at 10120 (U32)
Table 3.3 defines bitwise resolution.

### ErrorCode1 = FAULT_FLAG1 (reg 10112/0x2780, U32) - BITS
| Bit | Name |
|-----|------|
| 0 | GRID_LOSS |
| 1 | GRID_VOLTAGE_FAULT |
| 2 | GRID_FREQUENCY_FAULT |
| 3 | DCI_FAULT |
| 4 | ISO_OVER_LIMITATION |
| 5 | GFCI_FAULT |
| 6 | PV_OVER_VOLTAGE |
| 7 | BUS_VOLTAGE_FAULT |
| 8 | INVERTER_OVER_TEMPERATURE |

Note: PDF Table 3.3 only defines BIT0-8 for FLAG1. Bits 9-31 not documented.

### ErrorCode2 = FAULT_FLAG2 (reg 10114/0x2782, U32) - BITS
| Bit | Name |
|-----|------|
| 1 | SPI_FAULT |
| 2 | E2_FAULT |
| 3 | GFCI_DEVICE_FAULT |
| 4 | AC_TRANSDUCER_FAULT |
| 5 | RELAY_CHECK_FAIL |
| 6 | INTERNAL_FAN_FAULT |
| 7 | EXTERNAL_FAN_FAULT |

Note: BIT0, BIT8-31 not documented in PDF.

### ErrorCode3 = FAULT_FLAG3 (reg 10120/0x2788, U32) - BITS
| Bit | Name |
|-----|------|
| 0 | SCI_FAULT |
| 1 | FLASH_FAULT |

Note: BIT2-31 not documented in PDF.

---

## Summary

| Vendor | Type | Register(s) | Format | Bits Defined |
|--------|------|-------------|--------|-------------|
| Solis | bitfield | 3096-3100 (5x U16) | 16-bit each | 48 bits (3 regs x 16) |
| Growatt | bitfield | 0x0028 (U32) + 0x0040 (U16) | mixed | 20 fault + 10 warning |
| CPS | bitfield | 0x101E (U32) | 32-bit | 32 bits |
| Sunways | bitfield | 10112/10114/10120 (3x U32) | 32-bit each | 9+7+2 = 18 bits |
