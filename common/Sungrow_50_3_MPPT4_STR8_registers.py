# -*- coding: utf-8 -*-
"""Sungrow 50kW 3-phase inverter register map.

Based on Sungrow-PV_ti_20230117_communication-protocol_v1.1.53 PDF.
Configuration: MPPT=4, String=8 (for 50kW class).

Generated from Solarize_50_3 template + PDF address corrections.
"""


class RegisterMap:
    """Sungrow ti register addresses per PDF v1.1.53."""

    # =========================================================================
    # Phase AC voltage / current (0x5019~0x5024)
    # PDF: A-B line voltage/phase A voltage = 0x5019 etc.
    # =========================================================================
    L1_VOLTAGE                               = 0x5019  # U16, scale V 0.1
    L2_VOLTAGE                               = 0x5020  # U16, scale V 0.1
    L3_VOLTAGE                               = 0x5021  # U16, scale V 0.1
    L1_CURRENT                               = 0x5022  # U16, scale A 0.1
    L2_CURRENT                               = 0x5023  # U16, scale A 0.1
    L3_CURRENT                               = 0x5024  # U16, scale A 0.1

    # =========================================================================
    # MPPT inputs (0x5011~0x5016 + 0x5115~0x5116)
    # =========================================================================
    MPPT1_VOLTAGE                            = 0x5011  # U16, scale V 0.1
    MPPT1_CURRENT                            = 0x5012  # U16, scale A 0.1
    MPPT2_VOLTAGE                            = 0x5013  # U16, scale V 0.1
    MPPT2_CURRENT                            = 0x5014  # U16, scale A 0.1
    MPPT3_VOLTAGE                            = 0x5015  # U16, scale V 0.1
    MPPT3_CURRENT                            = 0x5016  # U16, scale A 0.1
    MPPT4_VOLTAGE                            = 0x5115  # U16, scale V 0.1 (separate block)
    MPPT4_CURRENT                            = 0x5116  # U16, scale A 0.1

    # =========================================================================
    # Total power / energy (0x5017, 0x5031, 0x5095)
    # =========================================================================
    PV_POWER                                 = 0x5017  # U32, total DC power, W
    PV_POWER_HIGH                            = 0x5018
    AC_POWER                                 = 0x5031  # U32, total active power, W
    AC_POWER_HIGH                            = 0x5032
    REACTIVE_POWER                           = 0x5033  # S32, Var
    REACTIVE_POWER_HIGH                      = 0x5034
    POWER_FACTOR                             = 0x5035  # S16, scale 0.001
    FREQUENCY                                = 0x5036  # U16, scale Hz 0.1

    # Cumulative energy (Total export) 0x5095-0x5096 (0.1 kWh)
    # Note: PDF scale is 0.1 kWh, but we use kWh for sim consistency
    CUMULATIVE_ENERGY                        = 0x5095  # U32, kWh (sim)
    CUMULATIVE_ENERGY_HIGH                   = 0x5096
    TOTAL_ENERGY                             = CUMULATIVE_ENERGY  # alias

    # =========================================================================
    # Status / Error codes (0x5038, 0x5045)
    # =========================================================================
    INVERTER_MODE                            = 0x5038  # U16, work state
    WORK_STATE                               = 0x5038
    ERROR_CODE1                              = 0x5045  # U16, fault/alarm code 1
    ERROR_CODE2                              = 0x5045  # alias (only 1 code in ti)
    ERROR_CODE3                              = 0x5045  # alias
    FAULT_CODE_1                             = 0x5045

    # =========================================================================
    # Temperature
    # =========================================================================
    INNER_TEMP                               = 0x502D  # S16, internal temp
    TEMPERATURE                              = INNER_TEMP

    # =========================================================================
    # String currents (0x7013~0x701A for strings 1-8)
    # Sungrow ti: CURRENT ONLY, no string voltage
    # =========================================================================
    STRING1_CURRENT                          = 0x7013  # U16, scale A 0.01
    STRING2_CURRENT                          = 0x7014  # U16
    STRING3_CURRENT                          = 0x7015  # U16
    STRING4_CURRENT                          = 0x7016  # U16
    STRING5_CURRENT                          = 0x7017  # U16
    STRING6_CURRENT                          = 0x7018  # U16
    STRING7_CURRENT                          = 0x7019  # U16
    STRING8_CURRENT                          = 0x701A  # U16

    # String voltage has no dedicated register in Sungrow ti - alias to MPPT
    STRING1_VOLTAGE                          = MPPT1_VOLTAGE
    STRING2_VOLTAGE                          = MPPT1_VOLTAGE
    STRING3_VOLTAGE                          = MPPT2_VOLTAGE
    STRING4_VOLTAGE                          = MPPT2_VOLTAGE
    STRING5_VOLTAGE                          = MPPT3_VOLTAGE
    STRING6_VOLTAGE                          = MPPT3_VOLTAGE
    STRING7_VOLTAGE                          = MPPT4_VOLTAGE
    STRING8_VOLTAGE                          = MPPT4_VOLTAGE

    # =========================================================================
    # DER-AVM registers (Solarize convention, shared across all inverters)
    # =========================================================================
    DEA_L1_CURRENT_LOW                       = 0x03E8  # S32, scale 0.1 A
    DEA_L1_CURRENT_HIGH                      = 0x03E9
    DEA_L2_CURRENT_LOW                       = 0x03EA
    DEA_L2_CURRENT_HIGH                      = 0x03EB
    DEA_L3_CURRENT_LOW                       = 0x03EC
    DEA_L3_CURRENT_HIGH                      = 0x03ED
    DEA_L1_VOLTAGE_LOW                       = 0x03EE  # S32, scale 0.1 V
    DEA_L1_VOLTAGE_HIGH                      = 0x03EF
    DEA_L2_VOLTAGE_LOW                       = 0x03F0
    DEA_L2_VOLTAGE_HIGH                      = 0x03F1
    DEA_L3_VOLTAGE_LOW                       = 0x03F2
    DEA_L3_VOLTAGE_HIGH                      = 0x03F3
    DEA_TOTAL_ACTIVE_POWER_LOW               = 0x03F4  # S32, 0.1 kW
    DEA_TOTAL_ACTIVE_POWER_HIGH              = 0x03F5
    DEA_TOTAL_REACTIVE_POWER_LOW             = 0x03F6  # S32
    DEA_TOTAL_REACTIVE_POWER_HIGH            = 0x03F7
    DEA_POWER_FACTOR_LOW                     = 0x03F8  # S32, 0.001
    DEA_POWER_FACTOR_HIGH                    = 0x03F9
    DEA_FREQUENCY_LOW                        = 0x03FA  # S32, 0.1 Hz
    DEA_FREQUENCY_HIGH                       = 0x03FB
    DEA_STATUS_FLAG_LOW                      = 0x03FC
    DEA_STATUS_FLAG_HIGH                     = 0x03FD

    # DER-AVM write registers (Solarize convention)
    DER_POWER_FACTOR_SET                     = 0x07D0
    DER_ACTION_MODE                          = 0x07D1
    DER_REACTIVE_POWER_PCT                   = 0x07D2
    DER_ACTIVE_POWER_PCT                     = 0x07D3
    INVERTER_ON_OFF                          = 0x0834
    MPPT_COUNT                               = 4
    NOMINAL_POWER_LOW                        = 0x0017
    NOMINAL_POWER_HIGH                       = 0x0018

    # Aliases for H01 field mapping
    R_PHASE_VOLTAGE                          = L1_VOLTAGE
    S_PHASE_VOLTAGE                          = L2_VOLTAGE
    T_PHASE_VOLTAGE                          = L3_VOLTAGE
    R_PHASE_CURRENT                          = L1_CURRENT
    S_PHASE_CURRENT                          = L2_CURRENT
    T_PHASE_CURRENT                          = L3_CURRENT
    R_VOLTAGE                                = L1_VOLTAGE
    R_CURRENT                                = L1_CURRENT
    ACTIVE_POWER                             = AC_POWER
    PV_VOLTAGE                               = MPPT1_VOLTAGE

    # IV Scan constants (not supported by Sungrow ti, but keep for compat)
    IV_SCAN_DATA_POINTS                      = 64
    IV_TRACKER_BLOCK_SIZE                    = 0x140
    IV_SCAN_COMMAND                          = 0x600D
    IV_SCAN_STATUS                           = 0x600D


    # Device info (FC04) — Sungrow String Inverter v1.1.37
    # DEVICE_MODEL: U16 type code at 0x1388 (Appendix 6 lookup)
    # Serial: UTF-8 STRING 10 regs at 0x137E (4990)
    DEVICE_MODEL                             = 0x1388
    DEVICE_MODEL_SIZE                        = 1
    DEVICE_SERIAL_NUMBER                     = 0x137E
    SERIAL_NUMBER = DEVICE_SERIAL_NUMBER                    
    DEVICE_SERIAL_NUMBER_SIZE                = 10


class InverterMode:
    """Sungrow work state (0x5038). PDF Appendix 1."""
    INITIAL  = 0x0000
    STANDBY  = 0x0008
    ON_GRID  = 0x0000  # Running
    STARTUP  = 0x0010
    FAULT    = 0x5500
    ALARM    = 0x9100
    SHUTDOWN = 0x8000

    @classmethod
    def to_string(cls, v):
        return {
            0x0000: 'RUNNING',
            0x0008: 'STANDBY',
            0x0010: 'STARTUP',
            0x5500: 'FAULT',
            0x9100: 'ALARM',
            0x8000: 'SHUTDOWN',
        }.get(v, f'UNKNOWN({v:#x})')


class IVScanCommand:
    NON_ACTIVE = 0
    ACTIVE = 1


class IVScanStatus:
    IDLE = 0
    RUNNING = 1
    FINISHED = 2

    @classmethod
    def to_string(cls, v):
        return {0: 'IDLE', 1: 'RUNNING', 2: 'FINISHED'}.get(v, f'UNKNOWN({v})')


class DerActionMode:
    NONE = 0
    POWER_FACTOR = 1
    REACTIVE_POWER = 2


class DeviceType:
    INVERTER = 1


class ControlMode:
    PF = 0
    RP = 1


class ErrorCode1:
    """Fault code (addr 0x5045, U16 enum -- not bitfield).
    Sungrow uses single enumerated fault codes, not bit flags.
    See FAULT_CODE_TABLE for code-to-description mapping."""
    BITS = {}
    FAULT_CODE_TABLE = {
        0x0002: 'Grid overvoltage',
        0x0003: 'Grid transient overvoltage',
        0x0004: 'Grid undervoltage',
        0x0005: 'Grid low voltage',
        0x0007: 'AC instantaneous overcurrent',
        0x0008: 'Grid over frequency',
        0x0009: 'Grid underfrequency',
        0x000A: 'Grid power outage',
        0x000B: 'Device abnormal',
        0x000C: 'Excessive leakage current',
        0x000D: 'Grid abnormal',
        0x000E: '10-minute grid overvoltage',
        0x000F: 'Grid high voltage',
        0x0010: 'Output overload',
        0x0011: 'Grid voltage unbalance',
        0x0013: 'Device abnormal (19)',
        0x0014: 'Device abnormal (20)',
        0x0015: 'Device abnormal (21)',
        0x0016: 'Device abnormal (22)',
        0x0017: 'PV connection fault',
        0x0024: 'Module temperature too high',
        0x0025: 'Ambient temperature too high',
        0x0027: 'Low system insulation resistance',
        0x002B: 'Low ambient temperature',
        0x002F: 'PV input config abnormal',
    }


class ErrorCode2:
    """Alias of ErrorCode1 (same register)."""
    BITS = {}


class ErrorCode3:
    """Alias of ErrorCode1 (same register)."""
    BITS = {}


class SungrowStatusConverter:
    @staticmethod
    def to_inverter_mode(raw):
        if raw == InverterMode.ON_GRID:
            return InverterMode.ON_GRID
        if raw == InverterMode.STANDBY:
            return InverterMode.STANDBY
        if raw == InverterMode.FAULT:
            return InverterMode.FAULT
        return raw


SCALE = {
    'voltage':             0.1,
    'current':             0.1,   # Sungrow uses 0.1A for phase/MPPT (not 0.01)
    'power':               1.0,   # Sungrow total power in W (no 0.1 scale)
    'frequency':           0.1,   # 0.1 Hz
    'power_factor':        0.001,
    'string_current':      0.01,  # String currents are 0.01A
    'dea_current':         0.1,
    'dea_voltage':         0.1,
    'dea_active_power':    0.1,
    'dea_reactive_power':  1.0,
    'dea_frequency':       0.1,
    'iv_voltage':          0.1,
    'iv_current':          0.1,
}


# Channel configuration
MPPT_CHANNELS = 4
STRING_CHANNELS = 8


def registers_to_u32(low, high):
    return (high << 16) | low


def registers_to_s32(low, high):
    value = (high << 16) | low
    if value >= 0x80000000:
        value -= 0x100000000
    return value


def get_mppt_registers(mppt_num):
    """Return (voltage, current, power_low, power_high) for MPPT 1-4.

    Sungrow has NO per-MPPT power register. Returns power addresses
    pointing to PV_POWER (total DC power) for all MPPTs as fallback.
    """
    if mppt_num < 1 or mppt_num > 4:
        raise ValueError(f"MPPT number must be 1-4, got {mppt_num}")
    if mppt_num <= 3:
        # MPPT1~3 at 0x5011, stride 2 (V, I pair)
        base = 0x5011 + (mppt_num - 1) * 2
    else:  # MPPT4
        base = 0x5115
    # No per-MPPT power in Sungrow — return total PV_POWER address
    return (base, base + 1, RegisterMap.PV_POWER, RegisterMap.PV_POWER_HIGH)


def get_string_registers(string_num):
    """Return (voltage_addr, current_addr) for string 1-8.

    Sungrow has NO string voltage. Returns (MPPT voltage alias, string current).
    """
    if string_num < 1 or string_num > 8:
        raise ValueError(f"String number must be 1-8, got {string_num}")
    current_base = 0x7013 + (string_num - 1)
    # String voltage fallback to corresponding MPPT voltage (2 strings/MPPT)
    mppt_num = (string_num - 1) // 2 + 1
    v_addrs = {1: 0x5011, 2: 0x5013, 3: 0x5015, 4: 0x5115}
    return (v_addrs[mppt_num], current_base)


def get_iv_tracker_voltage_registers(tracker_num, data_points=64):
    """Sungrow does not support IV scan. Returns stub."""
    return {'base': 0, 'count': data_points, 'end': data_points - 1}


def get_iv_string_current_registers(mppt_num, string_num, data_points=64):
    """Sungrow does not support IV scan. Returns stub."""
    return {'base': 0, 'count': data_points, 'end': data_points - 1}


def get_iv_string_mapping(total_strings=8, strings_per_mppt=2):
    """Sungrow does not support IV scan. Returns empty mapping."""
    return []


def generate_iv_voltage_data(voc, v_min, data_points=64):
    return [0] * data_points


def generate_iv_current_data(isc, voc, v_min, data_points=64):
    return [0] * data_points


DATA_TYPES = {
    'L1_VOLTAGE': 'U16',
    'L2_VOLTAGE': 'U16',
    'L3_VOLTAGE': 'U16',
    'L1_CURRENT': 'U16',
    'L2_CURRENT': 'U16',
    'L3_CURRENT': 'U16',
    'MPPT1_VOLTAGE': 'U16',
    'MPPT1_CURRENT': 'U16',
    'MPPT2_VOLTAGE': 'U16',
    'MPPT2_CURRENT': 'U16',
    'MPPT3_VOLTAGE': 'U16',
    'MPPT3_CURRENT': 'U16',
    'MPPT4_VOLTAGE': 'U16',
    'MPPT4_CURRENT': 'U16',
    'PV_POWER': 'U32',
    'AC_POWER': 'U32',
    'REACTIVE_POWER': 'S32',
    'POWER_FACTOR': 'S16',
    'FREQUENCY': 'U16',
    'CUMULATIVE_ENERGY': 'U32',
    'INVERTER_MODE': 'U16',
    'WORK_STATE': 'U16',
    'ERROR_CODE1': 'U16',
    'ERROR_CODE2': 'U16',
    'ERROR_CODE3': 'U16',
    'INNER_TEMP': 'S16',
    'STRING1_CURRENT': 'U16',
    'STRING2_CURRENT': 'U16',
    'STRING3_CURRENT': 'U16',
    'STRING4_CURRENT': 'U16',
    'STRING5_CURRENT': 'U16',
    'STRING6_CURRENT': 'U16',
    'STRING7_CURRENT': 'U16',
    'STRING8_CURRENT': 'U16',
    # DER-AVM
    'DEA_L1_CURRENT_LOW': 'S32',
    'DEA_L2_CURRENT_LOW': 'S32',
    'DEA_L3_CURRENT_LOW': 'S32',
    'DEA_L1_VOLTAGE_LOW': 'S32',
    'DEA_L2_VOLTAGE_LOW': 'S32',
    'DEA_L3_VOLTAGE_LOW': 'S32',
    'DEA_TOTAL_ACTIVE_POWER_LOW': 'S32',
    'DEA_TOTAL_REACTIVE_POWER_LOW': 'S32',
    'DEA_POWER_FACTOR_LOW': 'S32',
    'DEA_FREQUENCY_LOW': 'S32',
    'DEA_STATUS_FLAG_LOW': 'S32',
}


FLOAT32_FIELDS: set = set()
STRING_CURRENT_MONITOR = True


# RTU batch read blocks — Sungrow addresses
# Block 1: 0x03E8-0x03FD (DER-AVM registers, 22 regs)
# Block 2: 0x5011-0x5045 (MPPT1-3 + AC + power + status, 53 regs)
#          Includes MPPT1-3, Total DC power, L1-L3 V/I, Total AP, Reactive, PF, Freq, Work state, Fault
# Block 3: 0x5095-0x5096 (cumulative energy, 2 regs)
# Block 4: 0x5115-0x5116 (MPPT4, 2 regs)
# Block 5: 0x7013-0x701A (String1-8 current, 8 regs)
READ_BLOCKS = [
    {'start': 0x03E8, 'count':  22, 'fc': 3},
    {'start': 0x5011, 'count':  53, 'fc': 3},
    {'start': 0x5095, 'count':   2, 'fc': 3},
    {'start': 0x5115, 'count':   2, 'fc': 3},
    {'start': 0x7013, 'count':   8, 'fc': 3},
]


# H01 field → RegisterMap attribute mapping (with trailing spaces for backwards compat)
DATA_PARSER = {
    'mode                ': 'INVERTER_MODE',
    'r_voltage           ': 'L1_VOLTAGE',
    's_voltage           ': 'L2_VOLTAGE',
    't_voltage           ': 'L3_VOLTAGE',
    'r_current           ': 'L1_CURRENT',
    's_current           ': 'L2_CURRENT',
    't_current           ': 'L3_CURRENT',
    'frequency           ': 'FREQUENCY',
    'ac_power            ': 'AC_POWER',
    'cumulative_energy   ': 'CUMULATIVE_ENERGY',
    'alarm1              ': 'ERROR_CODE1',
    'mppt1_voltage'        : 'MPPT1_VOLTAGE',
    'mppt1_current'        : 'MPPT1_CURRENT',
    'mppt2_voltage'        : 'MPPT2_VOLTAGE',
    'mppt2_current'        : 'MPPT2_CURRENT',
    'mppt3_voltage'        : 'MPPT3_VOLTAGE',
    'mppt3_current'        : 'MPPT3_CURRENT',
    'mppt4_voltage'        : 'MPPT4_VOLTAGE',
    'mppt4_current'        : 'MPPT4_CURRENT',
    'string1_current'      : 'STRING1_CURRENT',
    'string2_current'      : 'STRING2_CURRENT',
    'string3_current'      : 'STRING3_CURRENT',
    'string4_current'      : 'STRING4_CURRENT',
    'string5_current'      : 'STRING5_CURRENT',
    'string6_current'      : 'STRING6_CURRENT',
    'string7_current'      : 'STRING7_CURRENT',
    'string8_current'      : 'STRING8_CURRENT',
}


# H01 field → (RegisterMap attr, converter key)
H01_FIELD_MAP = {
    'mode                ': ('INVERTER_MODE', 'raw'),
    'r_voltage           ': ('L1_VOLTAGE', 'voltage_to_V'),
    's_voltage           ': ('L2_VOLTAGE', 'voltage_to_V'),
    't_voltage           ': ('L3_VOLTAGE', 'voltage_to_V'),
    'r_current           ': ('L1_CURRENT', 'current_to_01A'),
    's_current           ': ('L2_CURRENT', 'current_to_01A'),
    't_current           ': ('L3_CURRENT', 'current_to_01A'),
    'frequency           ': ('FREQUENCY', 'frequency_to_01Hz'),
    'ac_power            ': ('AC_POWER', 'power_to_W'),
    'pv_power            ': ('PV_POWER', 'power_to_W'),
    'inner_temp          ': ('INNER_TEMP', 'raw'),
    'power_factor        ': ('POWER_FACTOR', 'pf_raw'),
    'cumulative_energy   ': ('CUMULATIVE_ENERGY', 'energy_kwh_to_Wh'),
    'alarm1              ': ('ERROR_CODE1', 'raw'),
    'alarm2              ': ('ERROR_CODE2', 'raw'),
    'alarm3              ': ('ERROR_CODE3', 'raw'),
}


U32_WORD_ORDER = 'LH'
RTU_FC_CODE = 3


# Sungrow device type code → model name (Appendix 6 excerpt, simulator use)
MODEL_CODE_MAP = {
    0x0E01: 'SG50CX-SIM',
}
MODEL_CODE_DEFAULT = 0x0E01
