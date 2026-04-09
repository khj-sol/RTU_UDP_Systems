# -*- coding: utf-8 -*-
"""Solis 25-50K/50-70K 50kW 3-phase inverter register map.

Based on Solis-PV_799467197-RS485-MODBUS-Protocol-V19.pdf
Configuration: MPPT=4, Strings=8 (2 strings per MPPT)

Solis uses FC04 input registers for measurement (3000-3999 doc addresses).
PDF instructs "address - 1" wire offset, but for sim/RTU consistency we use
the document address as-is (sim and RTU read the same value).

Per-MPPT block at 3500-3533. Per-string currents at 3301-3308.
Per-MPPT power is NOT a register — compute V*I in firmware.
SCALE follows Solarize convention for sim+RTU consistency.
Word order is HL.
"""


class RegisterMap:
    # Device info
    PRODUCT_MODEL                            = 0x0BB8  # 3000
    DSP_VERSION                              = 0x0BB9
    HMI_VERSION                              = 0x0BBA
    AC_OUTPUT_TYPE                           = 0x0BBB
    DC_INPUT_TYPE                            = 0x0BBC
    SERIAL_NUMBER                            = 0x0BF5  # 3061
    DEVICE_MODEL                             = 0x1A00
    DEVICE_SERIAL_NUMBER                     = 0x1A10
    FIRMWARE_VERSION                         = DSP_VERSION

    # Power & energy (3005-3018)
    ACTIVE_POWER                             = 0x0BBD  # 3005, S32 HL, W
    ACTIVE_POWER_HIGH                        = 0x0BBE
    AC_POWER                                 = ACTIVE_POWER
    AC_POWER_HIGH                            = ACTIVE_POWER_HIGH
    DC_OUTPUT_POWER                          = 0x0BBF  # 3007 U32 HL, W
    DC_OUTPUT_POWER_HIGH                     = 0x0BC0
    PV_POWER                                 = DC_OUTPUT_POWER
    TOTAL_ENERGY                             = 0x0BC1  # 3009 U32 HL, kWh
    TOTAL_ENERGY_HIGH                        = 0x0BC2
    CUMULATIVE_ENERGY                        = TOTAL_ENERGY
    CUMULATIVE_ENERGY_HIGH                   = TOTAL_ENERGY_HIGH
    ENERGY_THIS_MONTH                        = 0x0BC3
    ENERGY_LAST_MONTH                        = 0x0BC5
    DAILY_ENERGY                             = 0x0BC7  # 3015 U16 0.1 kWh
    ENERGY_THIS_YEAR                         = 0x0BC9

    # Reactive / apparent / PF
    REACTIVE_POWER                           = 0x0BF0  # 3056 S32 HL Var
    REACTIVE_POWER_HIGH                      = 0x0BF1
    APPARENT_POWER                           = 0x0BF2  # 3058 S32 HL VA
    APPARENT_POWER_HIGH                      = 0x0BF3
    POWER_FACTOR                             = 0x0BF4  # 3060 S16 0.001

    # Grid AC (3034-3043)
    PHASE_A_VOLTAGE                          = 0x0BDA  # 3034
    PHASE_B_VOLTAGE                          = 0x0BDB
    PHASE_C_VOLTAGE                          = 0x0BDC
    PHASE_A_CURRENT                          = 0x0BDD
    PHASE_B_CURRENT                          = 0x0BDE
    PHASE_C_CURRENT                          = 0x0BDF
    FREQUENCY                                = 0x0BE3  # 3043
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

    # Status / fault
    INVERTER_STATUS                          = 0x0BE4  # 3044
    INVERTER_MODE                            = INVERTER_STATUS
    DEVICE_STATUS                            = INVERTER_STATUS
    OPERATING_STATE                          = INVERTER_STATUS
    ALARM_CODE                               = 0x0BD6  # 3030
    FAULT_CODE_01                            = 0x0C18  # 3096
    FAULT_CODE_02                            = 0x0C19
    FAULT_CODE_03                            = 0x0C1A
    FAULT_CODE_04                            = 0x0C1B
    FAULT_CODE_05                            = 0x0C1C
    FAULT_CODE                               = FAULT_CODE_01
    ERROR_CODE1                              = FAULT_CODE_01
    ERROR_CODE2                              = FAULT_CODE_02
    ERROR_CODE3                              = FAULT_CODE_03

    # Temperature (3042 primary)
    INTERNAL_TEMP                            = 0x0BE2  # 3042 S16 0.1 C
    INNER_TEMP                               = INTERNAL_TEMP

    # Per-MPPT (4 MPPT) — V at 3500-3503, I at 3530-3533
    MPPT1_VOLTAGE                            = 0x0DAC  # 3500
    MPPT2_VOLTAGE                            = 0x0DAD
    MPPT3_VOLTAGE                            = 0x0DAE
    MPPT4_VOLTAGE                            = 0x0DAF
    MPPT1_CURRENT                            = 0x0DCA  # 3530
    MPPT2_CURRENT                            = 0x0DCB
    MPPT3_CURRENT                            = 0x0DCC
    MPPT4_CURRENT                            = 0x0DCD
    PV1_VOLTAGE                              = MPPT1_VOLTAGE
    PV2_VOLTAGE                              = MPPT2_VOLTAGE
    PV3_VOLTAGE                              = MPPT3_VOLTAGE
    PV4_VOLTAGE                              = MPPT4_VOLTAGE
    PV1_CURRENT                              = MPPT1_CURRENT
    PV2_CURRENT                              = MPPT2_CURRENT
    PV3_CURRENT                              = MPPT3_CURRENT
    PV4_CURRENT                              = MPPT4_CURRENT
    PV_VOLTAGE                               = MPPT1_VOLTAGE

    # Per-string current (8 strings, 3301-3308)
    STRING1_CURRENT                          = 0x0CE5  # 3301
    STRING2_CURRENT                          = 0x0CE6
    STRING3_CURRENT                          = 0x0CE7
    STRING4_CURRENT                          = 0x0CE8
    STRING5_CURRENT                          = 0x0CE9
    STRING6_CURRENT                          = 0x0CEA
    STRING7_CURRENT                          = 0x0CEB
    STRING8_CURRENT                          = 0x0CEC

    # Control — vendor ON/OFF reg 3007 (0x0BBF) collides with DC_OUTPUT_POWER
    # so we route ON/OFF through the standard Solarize DER-AVM 0x0834 address
    # which the simulator initializes. Real Solis hardware would need a
    # specialized handler that issues FC06 writes of 0xBE/0xDE to 3007.
    INVERTER_ON_OFF                          = 0x0834
    ACTIVE_POWER_LIMIT                       = 0x0BEC  # 3052 (10000=100%) — real hw only
    POWER_LIMIT_ENABLE                       = 0x0BFE  # 3070 0xAA=enable
    REACTIVE_POWER_LIMIT                     = 0x0BEB  # 3051
    REACTIVE_POWER_SWITCH                    = 0x0BFF  # 3071
    POWER_FACTOR_SET                         = 0x0BED  # 3053 S16 0.001

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
    INITIAL  = 0x0001  # OpenRun/SoftRun
    STANDBY  = 0x0000  # Waiting/Grid Off
    ON_GRID  = 0x0003  # Generating
    FAULT    = 0x1000
    SHUTDOWN = 0x2000

    @classmethod
    def to_string(cls, v):
        if v is None:
            return 'UNKNOWN'
        if v == 0x0000:
            return 'WAITING'
        if v in (0x0001, 0x0002):
            return 'STARTING'
        if v == 0x0003:
            return 'GENERATING'
        if (v & 0xF000) == 0x1000:
            return 'FAULT'
        if (v & 0xF000) == 0x2000:
            return 'COMM_FAULT'
        return f'UNKNOWN({v:#06x})'


class SolisStatusConverter:
    @staticmethod
    def to_inverter_mode(raw):
        if raw is None:
            return InverterMode.STANDBY
        if raw == 0x0000:
            return InverterMode.STANDBY
        if raw in (0x0001, 0x0002):
            return InverterMode.INITIAL
        if raw == 0x0003:
            return InverterMode.ON_GRID
        if (raw & 0xF000) == 0x1000:
            return InverterMode.FAULT
        if (raw & 0xF000) == 0x2000:
            return InverterMode.FAULT
        return InverterMode.STANDBY


StatusConverter = SolisStatusConverter


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


# 4 MPPT, 8 strings (2/MPPT) — direct MPPT registers
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
    'PV1_VOLTAGE': 'U16', 'PV1_CURRENT': 'S16',
    'PV2_VOLTAGE': 'U16', 'PV2_CURRENT': 'S16',
    'PV3_VOLTAGE': 'U16', 'PV3_CURRENT': 'S16',
    'PV4_VOLTAGE': 'U16', 'PV4_CURRENT': 'S16',
    'MPPT1_VOLTAGE': 'U16', 'MPPT1_CURRENT': 'S16',
    'MPPT2_VOLTAGE': 'U16', 'MPPT2_CURRENT': 'S16',
    'MPPT3_VOLTAGE': 'U16', 'MPPT3_CURRENT': 'S16',
    'MPPT4_VOLTAGE': 'U16', 'MPPT4_CURRENT': 'S16',
    'STRING1_CURRENT': 'S16', 'STRING2_CURRENT': 'S16',
    'STRING3_CURRENT': 'S16', 'STRING4_CURRENT': 'S16',
    'STRING5_CURRENT': 'S16', 'STRING6_CURRENT': 'S16',
    'STRING7_CURRENT': 'S16', 'STRING8_CURRENT': 'S16',
    'PHASE_A_VOLTAGE': 'U16', 'PHASE_B_VOLTAGE': 'U16', 'PHASE_C_VOLTAGE': 'U16',
    'L1_VOLTAGE': 'U16', 'L2_VOLTAGE': 'U16', 'L3_VOLTAGE': 'U16',
    'PHASE_A_CURRENT': 'U16', 'PHASE_B_CURRENT': 'U16', 'PHASE_C_CURRENT': 'U16',
    'L1_CURRENT': 'U16', 'L2_CURRENT': 'U16', 'L3_CURRENT': 'U16',
    'ACTIVE_POWER': 'S32', 'AC_POWER': 'S32', 'REACTIVE_POWER': 'S32',
    'APPARENT_POWER': 'S32', 'DC_OUTPUT_POWER': 'U32', 'PV_POWER': 'U32',
    'POWER_FACTOR': 'S16', 'FREQUENCY': 'U16',
    'INTERNAL_TEMP': 'S16', 'INNER_TEMP': 'S16',
    'INVERTER_MODE': 'U16', 'INVERTER_STATUS': 'U16',
    'DEVICE_STATUS': 'U16', 'OPERATING_STATE': 'U16',
    'FAULT_CODE': 'U16', 'FAULT_CODE_01': 'U16',
    'ERROR_CODE1': 'U16', 'ERROR_CODE2': 'U16', 'ERROR_CODE3': 'U16',
    'TOTAL_ENERGY': 'U32', 'CUMULATIVE_ENERGY': 'U32', 'DAILY_ENERGY': 'U16',
    'DEA_L1_CURRENT_LOW': 'S32', 'DEA_L2_CURRENT_LOW': 'S32', 'DEA_L3_CURRENT_LOW': 'S32',
    'DEA_L1_VOLTAGE_LOW': 'S32', 'DEA_L2_VOLTAGE_LOW': 'S32', 'DEA_L3_VOLTAGE_LOW': 'S32',
    'DEA_TOTAL_ACTIVE_POWER_LOW': 'S32', 'DEA_TOTAL_REACTIVE_POWER_LOW': 'S32',
    'DEA_POWER_FACTOR_LOW': 'S32', 'DEA_FREQUENCY_LOW': 'S32',
    'DEA_STATUS_FLAG_LOW': 'S32',
}


FLOAT32_FIELDS: set = set()
STRING_CURRENT_MONITOR = True


# Block 1: DER-AVM (0x03E8-0x03FD, 22 regs)
# Block 2: power totals + grid AC + status + temp (0x0BBD-0x0BF5, 57 regs)
# Block 3: per-string currents (0x0CE5-0x0CFC, 24 regs)
# Block 4: MPPT V + I (0x0DAC-0x0DCD, 34 regs)
READ_BLOCKS = [
    {'start': 0x03E8, 'count': 22, 'fc': 4},
    {'start': 0x0BBD, 'count': 57, 'fc': 4},
    {'start': 0x0CE5, 'count': 24, 'fc': 4},
    {'start': 0x0DAC, 'count': 34, 'fc': 4},
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
RTU_FC_CODE = 4
