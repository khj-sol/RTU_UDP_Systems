# -*- coding: utf-8 -*-
"""Sunways STT-30KTL 30kW 3-phase inverter register map.

Based on Sunways-PV_879922609-Modbus-Protocol.pdf (V00.07)
Configuration: MPPT=3, Strings=6 (2 strings per MPPT)

PDF native gains: V/I gain 10, F gain 100, P gain 1000.
Uses Solarize SCALE convention for sim+RTU consistency.
All reads use FC03. Word order is HL.
"""


class RegisterMap:
    # Device info
    SERIAL_NUMBER                            = 0x2710
    MODEL_INFORMATION                        = 0x2718
    OUTPUT_MODE                              = 0x2719
    PROTOCOL_VERSION                         = 0x271A
    FIRMWARE_VERSION                         = 0x271B
    FIRMWARE_VERSION_HIGH                    = 0x271C

    # Status (10105, 10112-10120)
    WORKING_STATE                            = 0x2779
    INVERTER_MODE                            = WORKING_STATE
    DEVICE_STATUS                            = WORKING_STATE
    FAULT_FLAG1                              = 0x2780
    FAULT_FLAG1_HIGH                         = 0x2781
    FAULT_FLAG2                              = 0x2782
    FAULT_FLAG2_HIGH                         = 0x2783
    FAULT_FLAG3                              = 0x2788
    FAULT_FLAG3_HIGH                         = 0x2789
    FAULT_CODE                               = FAULT_FLAG1
    ERROR_CODE1                              = FAULT_FLAG1
    ERROR_CODE2                              = FAULT_FLAG2
    ERROR_CODE3                              = FAULT_FLAG3

    # Grid AC (11006-11015)
    GRID_VAB                                 = 0x2AFE
    GRID_VBC                                 = 0x2AFF
    GRID_VCA                                 = 0x2B00
    PHASE_A_VOLTAGE                          = 0x2B01
    PHASE_A_CURRENT                          = 0x2B02
    PHASE_B_VOLTAGE                          = 0x2B03
    PHASE_B_CURRENT                          = 0x2B04
    PHASE_C_VOLTAGE                          = 0x2B05
    PHASE_C_CURRENT                          = 0x2B06
    FREQUENCY                                = 0x2B07
    L1_VOLTAGE                               = PHASE_A_VOLTAGE
    L2_VOLTAGE                               = PHASE_B_VOLTAGE
    L3_VOLTAGE                               = PHASE_C_VOLTAGE
    L1_CURRENT                               = PHASE_A_CURRENT
    L2_CURRENT                               = PHASE_B_CURRENT
    L3_CURRENT                               = PHASE_C_CURRENT
    R_PHASE_VOLTAGE                          = L1_VOLTAGE
    S_PHASE_VOLTAGE                          = L2_VOLTAGE
    T_PHASE_VOLTAGE                          = L3_VOLTAGE
    R_PHASE_CURRENT                          = L1_CURRENT
    S_PHASE_CURRENT                          = L2_CURRENT
    T_PHASE_CURRENT                          = L3_CURRENT
    R_VOLTAGE                                = L1_VOLTAGE
    R_CURRENT                                = L1_CURRENT

    # Power & energy (11016-11031)
    ACTIVE_POWER                             = 0x2B08  # U32 HL
    ACTIVE_POWER_HIGH                        = 0x2B09
    AC_POWER                                 = ACTIVE_POWER
    AC_POWER_HIGH                            = ACTIVE_POWER_HIGH
    DAILY_ENERGY                             = 0x2B0A
    DAILY_ENERGY_HIGH                        = 0x2B0B
    TOTAL_ENERGY                             = 0x2B0C
    TOTAL_ENERGY_HIGH                        = 0x2B0D
    CUMULATIVE_ENERGY                        = TOTAL_ENERGY
    CUMULATIVE_ENERGY_HIGH                   = TOTAL_ENERGY_HIGH
    TOTAL_GENERATION_TIME                    = 0x2B0E
    TOTAL_GENERATION_TIME_HIGH               = 0x2B0F
    APPARENT_POWER                           = 0x2B10
    APPARENT_POWER_HIGH                      = 0x2B11
    REACTIVE_POWER                           = 0x2B12
    REACTIVE_POWER_HIGH                      = 0x2B13
    PV_POWER                                 = 0x2B14
    PV_POWER_HIGH                            = 0x2B15
    DC_POWER                                 = PV_POWER
    POWER_FACTOR                             = 0x2B16
    EFFICIENCY                               = 0x2B17

    # Temperature & DC bus (11032-11037)
    INTERNAL_TEMP                            = 0x2B18
    INNER_TEMP                               = 0x2B19
    BUS_VOLTAGE                              = 0x2B1C

    # Per-MPPT (11038-11043 V/I, 11062-11067 power)
    PV1_VOLTAGE                              = 0x2B26
    PV1_CURRENT                              = 0x2B27
    PV2_VOLTAGE                              = 0x2B28
    PV2_CURRENT                              = 0x2B29
    PV3_VOLTAGE                              = 0x2B2A
    PV3_CURRENT                              = 0x2B2B
    MPPT1_VOLTAGE                            = PV1_VOLTAGE
    MPPT1_CURRENT                            = PV1_CURRENT
    MPPT2_VOLTAGE                            = PV2_VOLTAGE
    MPPT2_CURRENT                            = PV2_CURRENT
    MPPT3_VOLTAGE                            = PV3_VOLTAGE
    MPPT3_CURRENT                            = PV3_CURRENT
    PV_VOLTAGE                               = PV1_VOLTAGE

    PV1_POWER                                = 0x2B3E
    PV1_POWER_HIGH                           = 0x2B3F
    PV2_POWER                                = 0x2B40
    PV2_POWER_HIGH                           = 0x2B41
    PV3_POWER                                = 0x2B42
    PV3_POWER_HIGH                           = 0x2B43

    # Per-string current (11050-11055, 6 strings)
    STRING1_CURRENT                          = 0x2B32
    STRING2_CURRENT                          = 0x2B33
    STRING3_CURRENT                          = 0x2B34
    STRING4_CURRENT                          = 0x2B35
    STRING5_CURRENT                          = 0x2B36
    STRING6_CURRENT                          = 0x2B37

    # Control (FC06/FC16, 25008-25120)
    SWITCH_ON_OFF                            = 0x61B0
    ACTIVE_POWER_LIMIT_W                     = 0x6218
    ACTIVE_POWER_LIMIT_PCT                   = 0x621A
    REACTIVE_POWER_LIMIT_VAR                 = 0x621C
    REACTIVE_POWER_LIMIT_PCT                 = 0x621E
    POWER_FACTOR_SETTING                     = 0x6220

    # DER-AVM (Solarize convention)
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

    DER_POWER_FACTOR_SET                     = 0x07D0
    DER_ACTION_MODE                          = 0x07D1
    DER_REACTIVE_POWER_PCT                   = 0x07D2
    DER_ACTIVE_POWER_PCT                     = 0x07D3
    INVERTER_ON_OFF                          = 0x0834


    # Device info (FC03) — Sunways PV Protocol V00.07
    # DEVICE_MODEL: U16 type code at 0x2718 (hi=MPPT count, lo=rating index)
    # Serial: STRING 8 regs at 0x2710 (10000)
    DEVICE_MODEL                             = 0x2718
    DEVICE_MODEL_SIZE                        = 1
    DEVICE_SERIAL_NUMBER                     = 0x2710
    DEVICE_SERIAL_NUMBER_SIZE                = 8


class InverterMode:
    INITIAL  = 0x01  # check
    STANDBY  = 0x00  # wait
    ON_GRID  = 0x02  # normal
    FAULT    = 0x03  # fault
    SHUTDOWN = 0x04  # flash

    @classmethod
    def to_string(cls, v):
        return {0x00: 'WAIT', 0x01: 'CHECK', 0x02: 'NORMAL',
                0x03: 'FAULT', 0x04: 'FLASH'}.get(v, f'UNKNOWN({v:#04x})')


class SunwaysStatusConverter:
    @staticmethod
    def to_inverter_mode(raw):
        if raw is None:
            return InverterMode.STANDBY
        return {0x00: InverterMode.STANDBY, 0x01: InverterMode.INITIAL,
                0x02: InverterMode.ON_GRID, 0x03: InverterMode.FAULT,
                0x04: InverterMode.SHUTDOWN}.get(raw, InverterMode.STANDBY)


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
    BITS = {}


class ErrorCode2:
    BITS = {}


class ErrorCode3:
    BITS = {}


SCALE = {
    'voltage':             0.1,
    'current':             0.01,
    'power':               0.1,
    'frequency':           0.01,
    'power_factor':        0.001,
    'energy_to_kwh':       1.0,
    'dea_current':         0.1,
    'dea_voltage':         0.1,
    'dea_active_power':    0.1,
    'dea_reactive_power':  1.0,
    'dea_frequency':       0.1,
}


# 3 MPPT, 6 strings (2/MPPT) — direct MPPT registers (no aggregation needed)
MPPT_CHANNELS = 3
STRING_CHANNELS = 6


def registers_to_u32(low, high):
    return (high << 16) | low


def registers_to_s32(low, high):
    value = (high << 16) | low
    if value >= 0x80000000:
        value -= 0x100000000
    return value


DATA_TYPES = {
    'PV1_VOLTAGE': 'U16', 'PV1_CURRENT': 'U16',
    'PV2_VOLTAGE': 'U16', 'PV2_CURRENT': 'U16',
    'PV3_VOLTAGE': 'U16', 'PV3_CURRENT': 'U16',
    'MPPT1_VOLTAGE': 'U16', 'MPPT1_CURRENT': 'U16',
    'MPPT2_VOLTAGE': 'U16', 'MPPT2_CURRENT': 'U16',
    'MPPT3_VOLTAGE': 'U16', 'MPPT3_CURRENT': 'U16',
    'PV1_POWER': 'U32', 'PV2_POWER': 'U32', 'PV3_POWER': 'U32',
    'STRING1_CURRENT': 'U16', 'STRING2_CURRENT': 'U16', 'STRING3_CURRENT': 'U16',
    'STRING4_CURRENT': 'U16', 'STRING5_CURRENT': 'U16', 'STRING6_CURRENT': 'U16',
    'PHASE_A_VOLTAGE': 'U16', 'PHASE_B_VOLTAGE': 'U16', 'PHASE_C_VOLTAGE': 'U16',
    'L1_VOLTAGE': 'U16', 'L2_VOLTAGE': 'U16', 'L3_VOLTAGE': 'U16',
    'PHASE_A_CURRENT': 'U16', 'PHASE_B_CURRENT': 'U16', 'PHASE_C_CURRENT': 'U16',
    'L1_CURRENT': 'U16', 'L2_CURRENT': 'U16', 'L3_CURRENT': 'U16',
    'GRID_VAB': 'U16', 'GRID_VBC': 'U16', 'GRID_VCA': 'U16',
    'ACTIVE_POWER': 'U32', 'AC_POWER': 'U32', 'REACTIVE_POWER': 'S32',
    'APPARENT_POWER': 'U32', 'PV_POWER': 'U32', 'DC_POWER': 'U32',
    'POWER_FACTOR': 'S16', 'FREQUENCY': 'U16', 'EFFICIENCY': 'U16',
    'INTERNAL_TEMP': 'S16', 'INNER_TEMP': 'S16', 'BUS_VOLTAGE': 'U16',
    'WORKING_STATE': 'U16', 'INVERTER_MODE': 'U16', 'DEVICE_STATUS': 'U16',
    'FAULT_CODE': 'U32', 'FAULT_FLAG1': 'U32', 'FAULT_FLAG2': 'U32',
    'FAULT_FLAG3': 'U32', 'ERROR_CODE1': 'U32', 'ERROR_CODE2': 'U32',
    'ERROR_CODE3': 'U32',
    'TOTAL_ENERGY': 'U32', 'CUMULATIVE_ENERGY': 'U32', 'DAILY_ENERGY': 'U32',
    'DEA_L1_CURRENT_LOW': 'S32', 'DEA_L2_CURRENT_LOW': 'S32', 'DEA_L3_CURRENT_LOW': 'S32',
    'DEA_L1_VOLTAGE_LOW': 'S32', 'DEA_L2_VOLTAGE_LOW': 'S32', 'DEA_L3_VOLTAGE_LOW': 'S32',
    'DEA_TOTAL_ACTIVE_POWER_LOW': 'S32', 'DEA_TOTAL_REACTIVE_POWER_LOW': 'S32',
    'DEA_POWER_FACTOR_LOW': 'S32', 'DEA_FREQUENCY_LOW': 'S32',
    'DEA_STATUS_FLAG_LOW': 'S32',
}


FLOAT32_FIELDS: set = set()
STRING_CURRENT_MONITOR = True


# Block 1: DER-AVM (0x03E8-0x03FD, 22 regs)
# Block 2: Working state (0x2779, 1 reg)
# Block 3: Fault flags (0x2780-0x2789, 10 regs)
# Block 4: Grid + power + temp + MPPT V/I + string currents (0x2AFE-0x2B37, 58 regs)
# Block 5: PV1-3 power (0x2B3E-0x2B43, 6 regs)
READ_BLOCKS = [
    {'start': 0x03E8, 'count': 22, 'fc': 3},
    {'start': 0x2779, 'count':  1, 'fc': 3},
    {'start': 0x2780, 'count': 10, 'fc': 3},
    {'start': 0x2AFE, 'count': 58, 'fc': 3},
    {'start': 0x2B3E, 'count':  6, 'fc': 3},
]


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
    'string1_current'      : 'STRING1_CURRENT',
    'string2_current'      : 'STRING2_CURRENT',
    'string3_current'      : 'STRING3_CURRENT',
    'string4_current'      : 'STRING4_CURRENT',
    'string5_current'      : 'STRING5_CURRENT',
    'string6_current'      : 'STRING6_CURRENT',
}


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


U32_WORD_ORDER = 'HL'
RTU_FC_CODE = 3


# Sunways type code: high byte = MPPT count, low byte = rating index
# (15 = 3-phase four-MPPT, 1 = STT-30KTL per PDF)
MODEL_CODE_MAP = {
    0x0F01: 'STT-30KTL-SIM',
    0x1001: 'STT-50KTL-SIM',
}
MODEL_CODE_DEFAULT = 0x0F01
