# -*- coding: utf-8 -*-
"""CPS-PV / CPS-SCA-M 50kW 3-phase inverter register map.

Based on CPS-PV_CPS-SCA-M_Modbus_Protocol_V4.16_EN.pdf
Configuration: MPPT=3, Strings=9 (3 strings per MPPT)

CPS PDF only documents per-MPPT aggregate currents (PV1/PV2/PV3 Current).
There are no per-string current registers in the protocol document.
For sim/RTU per-string monitoring, synthetic STRING1..STRING9 current
addresses are exposed at 0x7000-0x7008 (U16, scale 0.01 A) — populated by
the simulator only; real hardware will return 0 if addressed.

All reads use FC03. Word order is HL.
"""


class RegisterMap:
    # Device info (0x1A00-0x1A48)
    DEVICE_MODEL                             = 0x1A00
    DEVICE_SERIAL_NUMBER                     = 0x1A10
    PROTOCOL_VERSION                         = 0x1A18
    SOFTWARE_VERSION                         = 0x1A1C
    SOFTWARE_BUILD_DATE                      = 0x1A23
    MPPT_NUMBER                              = 0x1A3B
    RATED_VOLTAGE                            = 0x1A44
    RATED_FREQUENCY                          = 0x1A45
    NOMINAL_POWER                            = 0x1A46
    GRID_PHASE_NUMBER                        = 0x1A48
    FIRMWARE_VERSION                         = SOFTWARE_VERSION

    # Status (0x101D, 0x101E)
    INVERTER_MODE                            = 0x101D
    DEVICE_STATUS                            = INVERTER_MODE
    OPERATING_STATE                          = INVERTER_MODE
    ERROR_CODE1                              = 0x101E  # U32 HL
    ERROR_CODE1_HIGH                         = 0x101F
    FAULT_CODE                               = ERROR_CODE1
    ERROR_CODE2                              = ERROR_CODE1
    ERROR_CODE3                              = ERROR_CODE1

    # Grid AC (0x1001-0x100F)
    PHASE_A_VOLTAGE                          = 0x1001
    PHASE_A_CURRENT                          = 0x1002
    PHASE_A_POWER                            = 0x1003  # U32 HL
    PHASE_A_POWER_HIGH                       = 0x1004
    PHASE_A_FREQUENCY                        = 0x1005
    PHASE_B_VOLTAGE                          = 0x1006
    PHASE_B_CURRENT                          = 0x1007
    PHASE_B_POWER                            = 0x1008
    PHASE_B_POWER_HIGH                       = 0x1009
    PHASE_B_FREQUENCY                        = 0x100A
    PHASE_C_VOLTAGE                          = 0x100B
    PHASE_C_CURRENT                          = 0x100C
    PHASE_C_POWER                            = 0x100D
    PHASE_C_POWER_HIGH                       = 0x100E
    PHASE_C_FREQUENCY                        = 0x100F
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
    FREQUENCY                                = PHASE_A_FREQUENCY

    # Energy / power totals (0x1021, 0x1027, 0x1037-0x103D)
    TOTAL_ENERGY                             = 0x1021  # U32 HL, kWh
    TOTAL_ENERGY_HIGH                        = 0x1022
    CUMULATIVE_ENERGY                        = TOTAL_ENERGY
    CUMULATIVE_ENERGY_HIGH                   = TOTAL_ENERGY_HIGH
    TOTAL_GENERATION_TIME                    = 0x1023
    TOTAL_GENERATION_TIME_HIGH               = 0x1024
    DAILY_ENERGY                             = 0x1027  # U32 HL, Wh
    DAILY_ENERGY_HIGH                        = 0x1028

    ACTIVE_POWER                             = 0x1037  # U32 HL, W
    ACTIVE_POWER_HIGH                        = 0x1038
    AC_POWER                                 = ACTIVE_POWER
    AC_POWER_HIGH                            = ACTIVE_POWER_HIGH
    REACTIVE_POWER                           = 0x1039  # S32 HL, Var
    REACTIVE_POWER_HIGH                      = 0x103A
    TODAY_PEAK_POWER                         = 0x103B
    TODAY_PEAK_POWER_HIGH                    = 0x103C
    POWER_FACTOR                             = 0x103D  # S16

    # Per-MPPT (0x1010-0x101A) — 3 MPPT
    PV1_VOLTAGE                              = 0x1010
    PV1_CURRENT                              = 0x1011
    MPPT1_POWER                              = 0x1012  # U32 HL
    MPPT1_POWER_HIGH                         = 0x1013
    PV2_VOLTAGE                              = 0x1014
    PV2_CURRENT                              = 0x1015
    MPPT2_POWER                              = 0x1016
    MPPT2_POWER_HIGH                         = 0x1017
    PV3_VOLTAGE                              = 0x1018
    PV3_CURRENT                              = 0x1019
    MPPT3_POWER                              = 0x101A
    MPPT3_POWER_HIGH                         = 0x101B
    MPPT1_VOLTAGE                            = PV1_VOLTAGE
    MPPT1_CURRENT                            = PV1_CURRENT
    MPPT2_VOLTAGE                            = PV2_VOLTAGE
    MPPT2_CURRENT                            = PV2_CURRENT
    MPPT3_VOLTAGE                            = PV3_VOLTAGE
    MPPT3_CURRENT                            = PV3_CURRENT
    PV1_POWER                                = MPPT1_POWER
    PV2_POWER                                = MPPT2_POWER
    PV3_POWER                                = MPPT3_POWER
    PV_VOLTAGE                               = PV1_VOLTAGE
    PV_POWER                                 = ACTIVE_POWER

    # Temperature (0x101C, S16)
    INNER_TEMP                               = 0x101C
    INTERNAL_TEMP                            = INNER_TEMP

    # Per-string currents — synthetic (PDF has no per-string regs).
    # Allocated at 0x7000-0x7008 for sim only. 9 strings (3/MPPT).
    STRING1_CURRENT                          = 0x7000
    STRING2_CURRENT                          = 0x7001
    STRING3_CURRENT                          = 0x7002
    STRING4_CURRENT                          = 0x7003
    STRING5_CURRENT                          = 0x7004
    STRING6_CURRENT                          = 0x7005
    STRING7_CURRENT                          = 0x7006
    STRING8_CURRENT                          = 0x7007
    STRING9_CURRENT                          = 0x7008

    # Control (FC06)
    INVERTER_ON_OFF                          = 0x6001  # 0=on, 1=shutdown
    ACTIVE_POWER_LIMIT                       = 0x5104  # 10..1000 = 1.0..100.0%
    REACTIVE_POWER_PCT                       = 0x5114  # 1..1000
    POWER_FACTOR_SET                         = 0x5031  # S16, [-1000,-800]u[800,1000]
    REGULATION_CODE                          = 0x5101

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


class InverterMode:
    INITIAL  = 0x00
    STANDBY  = 0x01
    ON_GRID  = 0x03
    FAULT    = 0x05
    SHUTDOWN = 0x09

    @classmethod
    def to_string(cls, v):
        return {0x00: 'INITIAL', 0x01: 'STANDBY', 0x03: 'ONLINE',
                0x05: 'FAULT', 0x09: 'SHUTDOWN'}.get(v, f'UNKNOWN({v:#04x})')


class CpsStatusConverter:
    @staticmethod
    def to_inverter_mode(raw):
        if raw is None:
            return InverterMode.STANDBY
        return {0x00: InverterMode.INITIAL, 0x01: InverterMode.STANDBY,
                0x03: InverterMode.ON_GRID, 0x05: InverterMode.FAULT,
                0x09: InverterMode.SHUTDOWN}.get(raw, InverterMode.STANDBY)


StatusConverter = CpsStatusConverter


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


# 3 MPPT, 9 strings (3/MPPT)
MPPT_CHANNELS = 3
STRING_CHANNELS = 9
STRINGS_PER_MPPT = 3


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
    'MPPT1_POWER': 'U32', 'MPPT2_POWER': 'U32', 'MPPT3_POWER': 'U32',
    'PV1_POWER': 'U32', 'PV2_POWER': 'U32', 'PV3_POWER': 'U32',
    'STRING1_CURRENT': 'U16', 'STRING2_CURRENT': 'U16', 'STRING3_CURRENT': 'U16',
    'STRING4_CURRENT': 'U16', 'STRING5_CURRENT': 'U16', 'STRING6_CURRENT': 'U16',
    'STRING7_CURRENT': 'U16', 'STRING8_CURRENT': 'U16', 'STRING9_CURRENT': 'U16',
    'PHASE_A_VOLTAGE': 'U16', 'PHASE_B_VOLTAGE': 'U16', 'PHASE_C_VOLTAGE': 'U16',
    'L1_VOLTAGE': 'U16', 'L2_VOLTAGE': 'U16', 'L3_VOLTAGE': 'U16',
    'PHASE_A_CURRENT': 'U16', 'PHASE_B_CURRENT': 'U16', 'PHASE_C_CURRENT': 'U16',
    'L1_CURRENT': 'U16', 'L2_CURRENT': 'U16', 'L3_CURRENT': 'U16',
    'PHASE_A_POWER': 'U32', 'PHASE_B_POWER': 'U32', 'PHASE_C_POWER': 'U32',
    'PHASE_A_FREQUENCY': 'U16', 'PHASE_B_FREQUENCY': 'U16', 'PHASE_C_FREQUENCY': 'U16',
    'ACTIVE_POWER': 'U32', 'AC_POWER': 'U32', 'REACTIVE_POWER': 'S32',
    'POWER_FACTOR': 'S16', 'FREQUENCY': 'U16',
    'INNER_TEMP': 'S16', 'INTERNAL_TEMP': 'S16',
    'INVERTER_MODE': 'U16', 'DEVICE_STATUS': 'U16', 'OPERATING_STATE': 'U16',
    'ERROR_CODE1': 'U32', 'ERROR_CODE2': 'U32', 'ERROR_CODE3': 'U32',
    'FAULT_CODE': 'U32',
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
# Block 2: AC + MPPT V/I/P + status + temp + error (0x1001-0x101F, 31 regs)
# Block 3: Energy/power totals (0x1021-0x103D, 29 regs)
# Block 4: Synthetic strings (0x7000-0x7008, 9 regs)
READ_BLOCKS = [
    {'start': 0x03E8, 'count': 22, 'fc': 3},
    {'start': 0x1001, 'count': 31, 'fc': 3},
    {'start': 0x1021, 'count': 29, 'fc': 3},
    {'start': 0x7000, 'count':  9, 'fc': 3},
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
    'string7_current'      : 'STRING7_CURRENT',
    'string8_current'      : 'STRING8_CURRENT',
    'string9_current'      : 'STRING9_CURRENT',
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
