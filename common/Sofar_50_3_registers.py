# -*- coding: utf-8 -*-
"""Sofar 1-70KTL G1/G2 50kW 3-phase inverter register map.

Based on SOFAR-PV_SOFAR_1-70KTL_G1-G2_Modbus_Protocol_EN_2021-01-27.pdf
Configuration: MPPT=4, Strings=8 (2 strings per MPPT)

Sofar PDF only documents PV1/PV2 inverter-side registers.
For 4-MPPT 50KTL units, the built-in DC combiner (0x0100-0x0114) exposes
8 string V/I pairs which map to MPPT1..MPPT4 in pairs (Strings 1-2 → MPPT1,
3-4 → MPPT2, 5-6 → MPPT3, 7-8 → MPPT4). modbus_handler aggregates strings →
MPPT via STRINGS_PER_MPPT=2.

All reads use FC03. Word order is HL.
"""


class RegisterMap:
    OPERATING_STATE                          = 0x0000
    INVERTER_MODE                            = OPERATING_STATE
    DEVICE_STATUS                            = OPERATING_STATE
    FAULT1                                   = 0x0001
    FAULT2                                   = 0x0002
    FAULT3                                   = 0x0003
    FAULT4                                   = 0x0004
    FAULT5                                   = 0x0005
    INVERTER_ALERT                           = 0x0021
    FAULT_CODE                               = FAULT1
    ERROR_CODE1                              = FAULT1
    ERROR_CODE2                              = FAULT2
    ERROR_CODE3                              = FAULT3

    PV1_VOLTAGE                              = 0x0006
    PV1_CURRENT                              = 0x0007
    PV2_VOLTAGE                              = 0x0008
    PV2_CURRENT                              = 0x0009
    PV1_POWER                                = 0x000A
    PV2_POWER                                = 0x000B
    PV_VOLTAGE                               = PV1_VOLTAGE

    ACTIVE_POWER                             = 0x000C
    AC_POWER                                 = ACTIVE_POWER
    REACTIVE_POWER                           = 0x000D
    FREQUENCY                                = 0x000E
    PHASE_A_VOLTAGE                          = 0x000F
    PHASE_A_CURRENT                          = 0x0010
    PHASE_B_VOLTAGE                          = 0x0011
    PHASE_B_CURRENT                          = 0x0012
    PHASE_C_VOLTAGE                          = 0x0013
    PHASE_C_CURRENT                          = 0x0014
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
    POWER_FACTOR                             = 0x000D  # PDF: no PF reg

    TOTAL_PRODUCTION                         = 0x0015  # U32 HL
    CUMULATIVE_ENERGY                        = 0x0015
    TOTAL_ENERGY                             = 0x0015
    DAILY_ENERGY                             = 0x0019

    INTERNAL_TEMP                            = 0x001B
    INNER_TEMP                               = 0x001C
    BUS_VOLTAGE                              = 0x001D

    STRING1_VOLTAGE                          = 0x0105
    STRING1_CURRENT                          = 0x0106
    STRING2_VOLTAGE                          = 0x0107
    STRING2_CURRENT                          = 0x0108
    STRING3_VOLTAGE                          = 0x0109
    STRING3_CURRENT                          = 0x010A
    STRING4_VOLTAGE                          = 0x010B
    STRING4_CURRENT                          = 0x010C
    STRING5_VOLTAGE                          = 0x010D
    STRING5_CURRENT                          = 0x010E
    STRING6_VOLTAGE                          = 0x010F
    STRING6_CURRENT                          = 0x0110
    STRING7_VOLTAGE                          = 0x0111
    STRING7_CURRENT                          = 0x0112
    STRING8_VOLTAGE                          = 0x0113
    STRING8_CURRENT                          = 0x0114

    ACTIVE_POWER_LIMIT                       = 0x9000

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


class InverterMode:
    INITIAL  = 0x01
    STANDBY  = 0x00
    ON_GRID  = 0x02
    FAULT    = 0x03
    SHUTDOWN = 0x04

    @classmethod
    def to_string(cls, v):
        return {0x00: 'WAIT', 0x01: 'CHECK', 0x02: 'NORMAL',
                0x03: 'FAULT', 0x04: 'PERMANENT'}.get(v, f'UNKNOWN({v:#04x})')


class SofarStatusConverter:
    @staticmethod
    def to_inverter_mode(raw):
        if raw is None:
            return InverterMode.STANDBY
        raw = raw & 0xFF
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


MPPT_CHANNELS = 4
STRING_CHANNELS = 8
STRINGS_PER_MPPT = 2


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
    'STRING1_VOLTAGE': 'U16', 'STRING1_CURRENT': 'U16',
    'STRING2_VOLTAGE': 'U16', 'STRING2_CURRENT': 'U16',
    'STRING3_VOLTAGE': 'U16', 'STRING3_CURRENT': 'U16',
    'STRING4_VOLTAGE': 'U16', 'STRING4_CURRENT': 'U16',
    'STRING5_VOLTAGE': 'U16', 'STRING5_CURRENT': 'U16',
    'STRING6_VOLTAGE': 'U16', 'STRING6_CURRENT': 'U16',
    'STRING7_VOLTAGE': 'U16', 'STRING7_CURRENT': 'U16',
    'STRING8_VOLTAGE': 'U16', 'STRING8_CURRENT': 'U16',
    'PHASE_A_VOLTAGE': 'U16', 'PHASE_B_VOLTAGE': 'U16', 'PHASE_C_VOLTAGE': 'U16',
    'L1_VOLTAGE': 'U16', 'L2_VOLTAGE': 'U16', 'L3_VOLTAGE': 'U16',
    'PHASE_A_CURRENT': 'U16', 'PHASE_B_CURRENT': 'U16', 'PHASE_C_CURRENT': 'U16',
    'L1_CURRENT': 'U16', 'L2_CURRENT': 'U16', 'L3_CURRENT': 'U16',
    'ACTIVE_POWER': 'U16', 'AC_POWER': 'U16', 'REACTIVE_POWER': 'U16',
    'POWER_FACTOR': 'U16', 'FREQUENCY': 'U16',
    'INTERNAL_TEMP': 'U16', 'INNER_TEMP': 'U16',
    'OPERATING_STATE': 'U16', 'INVERTER_MODE': 'U16', 'DEVICE_STATUS': 'U16',
    'FAULT_CODE': 'U16', 'FAULT1': 'U16', 'FAULT2': 'U16', 'FAULT3': 'U16',
    'ERROR_CODE1': 'U16', 'ERROR_CODE2': 'U16', 'ERROR_CODE3': 'U16',
    'TOTAL_PRODUCTION': 'U32', 'CUMULATIVE_ENERGY': 'U32', 'TOTAL_ENERGY': 'U32',
    'DAILY_ENERGY': 'U16',
    'DEA_L1_CURRENT_LOW': 'S32', 'DEA_L2_CURRENT_LOW': 'S32', 'DEA_L3_CURRENT_LOW': 'S32',
    'DEA_L1_VOLTAGE_LOW': 'S32', 'DEA_L2_VOLTAGE_LOW': 'S32', 'DEA_L3_VOLTAGE_LOW': 'S32',
    'DEA_TOTAL_ACTIVE_POWER_LOW': 'S32', 'DEA_TOTAL_REACTIVE_POWER_LOW': 'S32',
    'DEA_POWER_FACTOR_LOW': 'S32', 'DEA_FREQUENCY_LOW': 'S32',
    'DEA_STATUS_FLAG_LOW': 'S32',
}


FLOAT32_FIELDS: set = set()
STRING_CURRENT_MONITOR = True


READ_BLOCKS = [
    {'start': 0x03E8, 'count': 22, 'fc': 3},
    {'start': 0x0000, 'count': 30, 'fc': 3},
    {'start': 0x0021, 'count':  1, 'fc': 3},
    {'start': 0x0105, 'count': 16, 'fc': 3},
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
    'string1_current'      : 'STRING1_CURRENT',
    'string2_current'      : 'STRING2_CURRENT',
    'string3_current'      : 'STRING3_CURRENT',
    'string4_current'      : 'STRING4_CURRENT',
    'string5_current'      : 'STRING5_CURRENT',
    'string6_current'      : 'STRING6_CURRENT',
    'string7_current'      : 'STRING7_CURRENT',
    'string8_current'      : 'STRING8_CURRENT',
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
    'inner_temp          ': ('INNER_TEMP', 'raw'),
    'cumulative_energy   ': ('CUMULATIVE_ENERGY', 'energy_kwh_to_Wh'),
    'alarm1              ': ('ERROR_CODE1', 'raw'),
    'alarm2              ': ('ERROR_CODE2', 'raw'),
    'alarm3              ': ('ERROR_CODE3', 'raw'),
}


U32_WORD_ORDER = 'HL'
RTU_FC_CODE = 3
