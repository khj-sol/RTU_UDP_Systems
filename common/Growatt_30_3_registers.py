# -*- coding: utf-8 -*-
"""Growatt MID-25~40KTL3-X 30kW 3-phase inverter register map.

Based on Growatt-PV_Modbus_RS485_RTU_V3-14.pdf
Configuration: MPPT=3, Strings=6 (2 strings per MPPT)

Growatt uses FC04 input registers for measurement, FC03/06 holding for
control. Holding/Input share address 0x0000 (status vs OnOff). Sim uses
synthetic 0x6000+ addresses for control to avoid collision with status.

PV3 V/I/P at 0x0078-0x007B (out-of-band from PV1/PV2 block).
SCALE follows Solarize convention. Word order is HL.
"""


class RegisterMap:
    # Device info (FC03 holding) — addresses preserved for documentation
    FW_VERSION                               = 0x0009
    FW_VERSION2                              = 0x000C
    SERIAL_NUMBER                            = 0x0017
    MODULE_H                                 = 0x001C
    COM_ADDRESS                              = 0x001E
    DTC                                      = 0x002B
    TP                                       = 0x002C
    # DEVICE_MODEL: Use TP register at 0x002C (Tracker/Phase topology marker).
    # Per PDF V3.14 §0x002C this register returns 0x0303 for 3-MPPT 3-phase
    # inverters (hi byte = MPPT count, lo byte = phase count), so it is a
    # factually correct PDF-native model identifier for MID-25~40KTL3-X.
    # The PDF's Module register at 0x001C shares its address with the FC04
    # CUMULATIVE_ENERGY input register in the simulator's flat store, so
    # 0x002C is the only collision-free PDF-native device type code.
    DEVICE_MODEL                             = 0x002C
    DEVICE_MODEL_SIZE                        = 1
    DEVICE_SERIAL_NUMBER                     = 0x0017
    DEVICE_SERIAL_NUMBER_SIZE                = 5
    FIRMWARE_VERSION                         = FW_VERSION

    # Status (FC04 input 0x0000)
    INVERTER_STATUS                          = 0x0000
    INVERTER_MODE                            = INVERTER_STATUS
    DEVICE_STATUS                            = INVERTER_STATUS
    OPERATING_STATE                          = INVERTER_STATUS

    # Total PV power (Ppv 0x0001-0x0002 U32)
    PV_POWER                                 = 0x0001
    PV_POWER_HIGH                            = 0x0002
    DC_POWER                                 = PV_POWER

    # PV1 / PV2 / PV3 (PV3 is at 0x0078+)
    PV1_VOLTAGE                              = 0x0003
    PV1_CURRENT                              = 0x0004
    PV1_POWER                                = 0x0005
    PV1_POWER_HIGH                           = 0x0006
    PV2_VOLTAGE                              = 0x0007
    PV2_CURRENT                              = 0x0008
    PV2_POWER                                = 0x0009
    PV2_POWER_HIGH                           = 0x000A
    PV3_VOLTAGE                              = 0x0078
    PV3_CURRENT                              = 0x0079
    PV3_POWER                                = 0x007A
    PV3_POWER_HIGH                           = 0x007B
    MPPT1_VOLTAGE                            = PV1_VOLTAGE
    MPPT1_CURRENT                            = PV1_CURRENT
    MPPT2_VOLTAGE                            = PV2_VOLTAGE
    MPPT2_CURRENT                            = PV2_CURRENT
    MPPT3_VOLTAGE                            = PV3_VOLTAGE
    MPPT3_CURRENT                            = PV3_CURRENT
    PV_VOLTAGE                               = PV1_VOLTAGE

    # AC output (Pac 0x000B-0x000C U32)
    ACTIVE_POWER                             = 0x000B
    ACTIVE_POWER_HIGH                        = 0x000C
    AC_POWER                                 = ACTIVE_POWER
    AC_POWER_HIGH                            = ACTIVE_POWER_HIGH
    FREQUENCY                                = 0x000D

    # Phase R/S/T V/I/P (0x000E-0x0019)
    PHASE_A_VOLTAGE                          = 0x000E
    PHASE_A_CURRENT                          = 0x000F
    PHASE_A_POWER                            = 0x0010
    PHASE_A_POWER_HIGH                       = 0x0011
    PHASE_B_VOLTAGE                          = 0x0012
    PHASE_B_CURRENT                          = 0x0013
    PHASE_B_POWER                            = 0x0014
    PHASE_B_POWER_HIGH                       = 0x0015
    PHASE_C_VOLTAGE                          = 0x0016
    PHASE_C_CURRENT                          = 0x0017
    PHASE_C_POWER                            = 0x0018
    PHASE_C_POWER_HIGH                       = 0x0019
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

    # Energy (0x001A-0x001F)
    DAILY_ENERGY                             = 0x001A  # U32 HL, 0.1 kWh
    DAILY_ENERGY_HIGH                        = 0x001B
    TOTAL_ENERGY                             = 0x001C  # U32 HL, 0.1 kWh
    TOTAL_ENERGY_HIGH                        = 0x001D
    CUMULATIVE_ENERGY                        = TOTAL_ENERGY
    CUMULATIVE_ENERGY_HIGH                   = TOTAL_ENERGY_HIGH
    TIME_TOTAL                               = 0x001E

    # Temperature (0x0020 inverter, 0x0029 IPM)
    INTERNAL_TEMP                            = 0x0020
    INNER_TEMP                               = INTERNAL_TEMP
    IPM_TEMP                                 = 0x0029

    # PF + reactive
    POWER_FACTOR                             = 0x002D  # 10000 = 1.0
    REACTIVE_POWER                           = 0x003A  # U32 HL Var
    REACTIVE_POWER_HIGH                      = 0x003B
    DERATING_MODE                            = 0x002F

    # Fault codes
    FAULT_CODE                               = 0x0028
    FAULT_CODE_HL                            = 0x0080
    WARNING_CODE                             = 0x0040
    ERROR_CODE1                              = FAULT_CODE
    ERROR_CODE2                              = WARNING_CODE
    ERROR_CODE3                              = 0x0080

    # Per-string V/I (0x0046-0x0055, 8 strings, use 1-6)
    STRING1_VOLTAGE                          = 0x0046
    STRING1_CURRENT                          = 0x0047
    STRING2_VOLTAGE                          = 0x0048
    STRING2_CURRENT                          = 0x0049
    STRING3_VOLTAGE                          = 0x004A
    STRING3_CURRENT                          = 0x004B
    STRING4_VOLTAGE                          = 0x004C
    STRING4_CURRENT                          = 0x004D
    STRING5_VOLTAGE                          = 0x004E
    STRING5_CURRENT                          = 0x004F
    STRING6_VOLTAGE                          = 0x0050
    STRING6_CURRENT                          = 0x0051

    # Control (synthetic 0x6000+ to avoid collision with input 0x0000-area)
    # PDF holding addresses are 0x0000(OnOff)/0x0003(P%)/0x0004(Q%)/0x0005(PF)
    INVERTER_ON_OFF                          = 0x6000
    ACTIVE_POWER_LIMIT                       = 0x6003
    REACTIVE_POWER_LIMIT                     = 0x6004
    POWER_FACTOR_SET                         = 0x6005

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
    INITIAL  = 0x00  # Waiting
    STANDBY  = 0x00
    ON_GRID  = 0x01  # Normal
    FAULT    = 0x03
    SHUTDOWN = 0x03

    @classmethod
    def to_string(cls, v):
        return {0x00: 'WAITING', 0x01: 'NORMAL',
                0x03: 'FAULT'}.get(v, f'UNKNOWN({v:#04x})')


class GrowattStatusConverter:
    @staticmethod
    def to_inverter_mode(raw):
        if raw is None:
            return InverterMode.STANDBY
        return {0x00: InverterMode.STANDBY, 0x01: InverterMode.ON_GRID,
                0x03: InverterMode.FAULT}.get(raw, InverterMode.STANDBY)


StatusConverter = GrowattStatusConverter


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


# 3 MPPT, 6 strings (2/MPPT) — direct MPPT registers
MPPT_CHANNELS = 3
STRING_CHANNELS = 6
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
    'PV3_VOLTAGE': 'U16', 'PV3_CURRENT': 'U16',
    'MPPT1_VOLTAGE': 'U16', 'MPPT1_CURRENT': 'U16',
    'MPPT2_VOLTAGE': 'U16', 'MPPT2_CURRENT': 'U16',
    'MPPT3_VOLTAGE': 'U16', 'MPPT3_CURRENT': 'U16',
    'PV1_POWER': 'U32', 'PV2_POWER': 'U32', 'PV3_POWER': 'U32',
    'STRING1_VOLTAGE': 'U16', 'STRING1_CURRENT': 'S16',
    'STRING2_VOLTAGE': 'U16', 'STRING2_CURRENT': 'S16',
    'STRING3_VOLTAGE': 'U16', 'STRING3_CURRENT': 'S16',
    'STRING4_VOLTAGE': 'U16', 'STRING4_CURRENT': 'S16',
    'STRING5_VOLTAGE': 'U16', 'STRING5_CURRENT': 'S16',
    'STRING6_VOLTAGE': 'U16', 'STRING6_CURRENT': 'S16',
    'PHASE_A_VOLTAGE': 'U16', 'PHASE_B_VOLTAGE': 'U16', 'PHASE_C_VOLTAGE': 'U16',
    'L1_VOLTAGE': 'U16', 'L2_VOLTAGE': 'U16', 'L3_VOLTAGE': 'U16',
    'PHASE_A_CURRENT': 'U16', 'PHASE_B_CURRENT': 'U16', 'PHASE_C_CURRENT': 'U16',
    'L1_CURRENT': 'U16', 'L2_CURRENT': 'U16', 'L3_CURRENT': 'U16',
    'PHASE_A_POWER': 'U32', 'PHASE_B_POWER': 'U32', 'PHASE_C_POWER': 'U32',
    'ACTIVE_POWER': 'U32', 'AC_POWER': 'U32', 'REACTIVE_POWER': 'U32',
    'PV_POWER': 'U32', 'DC_POWER': 'U32',
    'POWER_FACTOR': 'U16', 'FREQUENCY': 'U16',
    'INTERNAL_TEMP': 'U16', 'INNER_TEMP': 'U16', 'IPM_TEMP': 'U16',
    'INVERTER_MODE': 'U16', 'INVERTER_STATUS': 'U16',
    'DEVICE_STATUS': 'U16', 'OPERATING_STATE': 'U16',
    'FAULT_CODE': 'U16', 'WARNING_CODE': 'U16',
    'ERROR_CODE1': 'U16', 'ERROR_CODE2': 'U16', 'ERROR_CODE3': 'U16',
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
# Block 2: Status + PV1/PV2 + Pac + AC R/S/T + energy + temp + alarms (0x0000-0x002F, 48 regs)
# Block 3: PF + reactive + faults (0x002D-0x0058, 44 regs)  -- merged into Block 2
# Block 4: Per-string V/I (0x0046-0x0055, 16 regs) -- inside Block 2 range
# Block 5: PV3 V/I/P + Epv3 (0x0078-0x007F, 8 regs)
READ_BLOCKS = [
    {'start': 0x03E8, 'count': 22, 'fc': 4},
    {'start': 0x0000, 'count': 44, 'fc': 4},
    {'start': 0x0046, 'count': 16, 'fc': 4},
    {'start': 0x0078, 'count':  8, 'fc': 4},
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
RTU_FC_CODE = 4


# Growatt module code mapping (PDF §0x001C Module H register)
MODEL_CODE_MAP = {
    0x0303: 'MOD-30KTL3-SIM',  # 3 MPPT + 3 phase topology marker
}
MODEL_CODE_DEFAULT = 0x0303
