# -*- coding: utf-8 -*-
"""Ekos-PV 10kW 3-phase inverter register map.

Based on Ekos-PV_ModBus_map-EK_20220209_통신테스트용.xlsx.
Configuration: MPPT=1 (central type), Strings=2.

Central type — only per-string V/I registers exist. MPPT V/I is derived
by rtu_program/modbus_handler.py via STRINGS_PER_MPPT=2 aggregation
(voltage = average of strings on MPPT, current = sum of strings).

Ekos uses FC04 (Read Input Registers). Register numbers in the Excel
are 30001-based (Modbus 3xxxx convention); Modbus address = reg - 30001.
All measurement values are IEEE754 Float32 stored in 2 consecutive regs.
"""


class RegisterMap:
    """Ekos-PV register addresses per Excel."""

    # =========================================================================
    # Device Info (30001-30024)
    # =========================================================================
    MODEL_NAME                               = 0x0000  # 30001 ASCII 8 regs
    SERIAL_NUMBER                            = 0x0008  # 30009 ASCII 8 regs
    INVERTER_VERSION                         = 0x0010  # 30017 U16
    INVERTER_CAPACITY                        = 0x0011  # 30018 U16 kW

    # =========================================================================
    # Status (30025-30029)
    # =========================================================================
    INVERTER_MODE                            = 0x0018  # 30025 U16 (F007)
    GENERATION_STATUS                        = 0x0019  # 30026 U16 (F008)
    INVERTER_STATUS                          = 0x001A  # 30027 U16 (F009)
    SW_FAULT_ALARM1                          = 0x001B  # 30028 U16 (F010)
    HW_FAULT_ALARM2                          = 0x001C  # 30029 U16 (F011)

    # =========================================================================
    # Today runtime / cumulative time (30030-30034)
    # =========================================================================
    DAILY_RUNTIME_SEC                        = 0x001D  # 30030 U16 sec
    DAILY_START_HM                           = 0x001E  # 30031 U16
    DAILY_STOP_HM                            = 0x001F  # 30032 U16
    ACCUMULATED_TIME                         = 0x0020  # 30033-34 U32 sec

    # =========================================================================
    # PV / Inverter DC (30035-30042)  Float32
    # =========================================================================
    PV_CELL_VOLTAGE                          = 0x0022  # 30035-36 태양전지 전압
    PV_VOLTAGE                               = 0x0024  # 30037-38 인버터 DC전압
    PV_CELL_CURRENT                          = 0x0026  # 30039-40 태양전지 전류
    PV_CURRENT                               = PV_CELL_CURRENT
    PV_POWER                                 = 0x0028  # 30041-42 태양전지 전력 W

    # =========================================================================
    # AC Total (30043-30058)  Float32
    # =========================================================================
    AC_VOLTAGE_TOTAL                         = 0x002A  # 30043-44
    AC_CURRENT_TOTAL                         = 0x002C  # 30045-46
    ACTIVE_POWER                             = 0x002E  # 30047-48  W
    AC_POWER                                 = ACTIVE_POWER
    REACTIVE_POWER                           = 0x0030  # 30049-50  VA
    POWER_FACTOR                             = 0x0032  # 30051-52  %
    FREQUENCY                                = 0x0034  # 30053-54  Hz
    DAILY_ENERGY                             = 0x0036  # 30055-56  Wh
    CUMULATIVE_ENERGY                        = 0x0038  # 30057-58  Wh (Float32)
    TOTAL_ENERGY                             = CUMULATIVE_ENERGY

    # =========================================================================
    # AC Per-phase (30059-30088)  Float32
    # =========================================================================
    L1_VOLTAGE                               = 0x003A  # 30059-60
    L2_VOLTAGE                               = 0x003C  # 30061-62
    L3_VOLTAGE                               = 0x003E  # 30063-64
    L1_CURRENT                               = 0x0040  # 30065-66
    L2_CURRENT                               = 0x0042  # 30067-68
    L3_CURRENT                               = 0x0044  # 30069-70
    L1_ACTIVE_POWER                          = 0x0046  # 30071-72 W
    L2_ACTIVE_POWER                          = 0x0048  # 30073-74
    L3_ACTIVE_POWER                          = 0x004A  # 30075-76
    L1_APPARENT_POWER                        = 0x004C  # 30077-78 VA
    L2_APPARENT_POWER                        = 0x004E  # 30079-80
    L3_APPARENT_POWER                        = 0x0050  # 30081-82
    L1_POWER_FACTOR                          = 0x0052  # 30083-84
    L2_POWER_FACTOR                          = 0x0054  # 30085-86
    L3_POWER_FACTOR                          = 0x0056  # 30087-88

    # =========================================================================
    # Temperature (30089-30092)  Float32
    # =========================================================================
    MPPT_TEMPERATURE                         = 0x0058  # 30089-90 PV MPPT temp
    INNER_TEMP                               = 0x005A  # 30091-92 Heatsink temp

    # =========================================================================
    # Accumulated energy (30095-30098)  U32
    # =========================================================================
    ACCUMULATED_ENERGY_MWH                   = 0x005E  # 30095-96 MWh
    ACCUMULATED_ENERGY_WH                    = 0x0060  # 30097-98 Wh (decimal)

    # =========================================================================
    # Fault maps (30099-30104)
    # =========================================================================
    PV_STATUS_TOTAL                          = 0x0062  # 30099
    PV1_STATUS_TOTAL                         = 0x0063  # 30100
    PV2_STATUS_TOTAL                         = 0x0064  # 30101
    INVERTER_STATUS_FAULT                    = 0x0065  # 30102
    GRID_STATUS_ALARM3                       = 0x0066  # 30103
    CONVERTER_STATUS                         = 0x0067  # 30104

    # =========================================================================
    # String-level V/I/P (30105-30116)  Float32
    # =========================================================================
    STRING1_VOLTAGE                          = 0x0068  # 30105-06
    STRING1_CURRENT                          = 0x006A  # 30107-08
    STRING1_POWER                            = 0x006C  # 30109-10
    STRING2_VOLTAGE                          = 0x006E  # 30111-12
    STRING2_CURRENT                          = 0x0070  # 30113-14
    STRING2_POWER                            = 0x0072  # 30115-16  (Excel typo 30105)

    # =========================================================================
    # Aliases for H01 output
    # =========================================================================
    R_PHASE_VOLTAGE                          = L1_VOLTAGE
    S_PHASE_VOLTAGE                          = L2_VOLTAGE
    T_PHASE_VOLTAGE                          = L3_VOLTAGE
    R_PHASE_CURRENT                          = L1_CURRENT
    S_PHASE_CURRENT                          = L2_CURRENT
    T_PHASE_CURRENT                          = L3_CURRENT
    R_VOLTAGE                                = L1_VOLTAGE
    R_CURRENT                                = L1_CURRENT

    ERROR_CODE1                              = SW_FAULT_ALARM1
    ERROR_CODE2                              = HW_FAULT_ALARM2
    ERROR_CODE3                              = GRID_STATUS_ALARM3

    # =========================================================================
    # DER-AVM (Solarize convention, for simulator compat)
    # =========================================================================
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


    # Device info (FC03) — Ekos EK PDF
    # Existing MODEL_NAME/SERIAL_NUMBER already at 0x0000/0x0008; alias for
    # the standardized DEVICE_MODEL/DEVICE_SERIAL_NUMBER lookup names.
    DEVICE_MODEL                             = 0x0000
    DEVICE_MODEL_SIZE                        = 8
    DEVICE_SERIAL_NUMBER                     = 0x0008
    DEVICE_SERIAL_NUMBER_SIZE                = 8


class InverterMode:
    """Ekos operating state (reg 30025) — F007 enum."""
    INITIAL  = 1   # Initializing
    STANDBY  = 0   # Stop
    WAITING  = 2
    ON_GRID  = 8   # MPP
    FAULT    = 20  # synthetic
    SHUTDOWN = 0

    @classmethod
    def to_string(cls, v):
        return {
            0: 'STOP',
            1: 'INITIALIZING',
            2: 'WAITING',
            3: 'PV_VOLTAGE_CHECK',
            4: 'AC_VOLTAGE_CHECK',
            5: 'MC_CLOSE',
            6: 'MPP_START',
            7: 'MPP_SEARCH',
            8: 'MPP',
        }.get(v, f'UNKNOWN({v})')


class EkosStatusConverter:
    @staticmethod
    def to_inverter_mode(raw):
        if raw is None:
            return InverterMode.STANDBY
        if raw == 8:
            return InverterMode.ON_GRID
        if raw == 0:
            return InverterMode.STANDBY
        if raw == 1:
            return InverterMode.INITIAL
        return InverterMode.STANDBY


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
    """SW Fault bitfield (F010)."""
    BITS = {
        0:  'VpvLvF',     # PV voltage under
        1:  'VpvOvF',     # PV voltage over
        2:  'VdcLvF',     # DC voltage under
        3:  'VdcOvF',     # DC voltage over
        4:  'VsLvF',      # Grid voltage under
        5:  'VsOvF',      # Grid voltage over
        6:  'CALerr',     # Calibration
        7:  'SETerr',     # Parameter setup
        8:  'CMDerr',     # Comm config
    }


class ErrorCode2:
    """HW Fault bitfield (F011)."""
    BITS = {
        0: 'IdcOcHw',     # PV overcurrent
        1: 'AcOcHw',      # AC overcurrent
        2: 'AcSYNC',      # AC frequency
        3: 'InvF',        # Inverter ctrl
        4: 'OtF',         # Heat overtemp
        5: 'ISLAND',      # Islanding
    }


class ErrorCode3:
    """GRID_STATUS_ALARM3 (reg 30103, U16 bitfield, Fault MAP)."""
    BITS = {
        0:  'GridOVR',
        1:  'GridUVR',
        2:  'GridOFR',
        3:  'GridUFR',
        4:  'GridFAIL',
        5:  'GridEARTH',
        6:  'GridRCMU',
        11: 'GridSPD',
        15: 'GridFaultAll',
    }


SCALE = {
    # Ekos uses Float32 for measurements. The simulator writes with these
    # scales to convert its internal "raw 0.1V / 0.01A" values to the
    # physical float stored in registers (e.g. raw 3800 × 0.1 = 380.0 V).
    # The RTU reader's Float32 H01 converters then undo the scale back to
    # H01 integer form.
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


# Channel configuration — 10kW central type: 1 MPPT, 2 strings
MPPT_CHANNELS = 1
STRING_CHANNELS = 2
STRINGS_PER_MPPT = 2


def registers_to_u32(low, high):
    return (high << 16) | low


def registers_to_s32(low, high):
    value = (high << 16) | low
    if value >= 0x80000000:
        value -= 0x100000000
    return value


DATA_TYPES = {
    # PV / strings — Float32
    'PV_CELL_VOLTAGE':  'FLOAT32',
    'PV_VOLTAGE':       'FLOAT32',
    'PV_CELL_CURRENT':  'FLOAT32',
    'PV_CURRENT':       'FLOAT32',
    'PV_POWER':         'FLOAT32',
    'STRING1_VOLTAGE':  'FLOAT32',
    'STRING1_CURRENT':  'FLOAT32',
    'STRING1_POWER':    'FLOAT32',
    'STRING2_VOLTAGE':  'FLOAT32',
    'STRING2_CURRENT':  'FLOAT32',
    'STRING2_POWER':    'FLOAT32',
    # Grid — Float32
    'AC_VOLTAGE_TOTAL': 'FLOAT32',
    'AC_CURRENT_TOTAL': 'FLOAT32',
    'ACTIVE_POWER':     'FLOAT32',
    'AC_POWER':         'FLOAT32',
    'REACTIVE_POWER':   'FLOAT32',
    'POWER_FACTOR':     'FLOAT32',
    'FREQUENCY':        'FLOAT32',
    'L1_VOLTAGE':       'FLOAT32',
    'L2_VOLTAGE':       'FLOAT32',
    'L3_VOLTAGE':       'FLOAT32',
    'L1_CURRENT':       'FLOAT32',
    'L2_CURRENT':       'FLOAT32',
    'L3_CURRENT':       'FLOAT32',
    'L1_ACTIVE_POWER':  'FLOAT32',
    'L2_ACTIVE_POWER':  'FLOAT32',
    'L3_ACTIVE_POWER':  'FLOAT32',
    'DAILY_ENERGY':     'FLOAT32',
    'CUMULATIVE_ENERGY': 'FLOAT32',
    'MPPT_TEMPERATURE': 'FLOAT32',
    'INNER_TEMP':       'FLOAT32',
    # U16 status / alarms
    'INVERTER_MODE':    'U16',
    'GENERATION_STATUS': 'U16',
    'INVERTER_STATUS':  'U16',
    'SW_FAULT_ALARM1':  'U16',
    'HW_FAULT_ALARM2':  'U16',
    'PV_STATUS_TOTAL':  'U16',
    'GRID_STATUS_ALARM3': 'U16',
    'ERROR_CODE1':      'U16',
    'ERROR_CODE2':      'U16',
    'ERROR_CODE3':      'U16',
    'DAILY_RUNTIME_SEC': 'U16',
    # U32
    'ACCUMULATED_TIME':         'U32',
    'ACCUMULATED_ENERGY_MWH':   'U32',
    'ACCUMULATED_ENERGY_WH':    'U32',
    # DER-AVM
    'DEA_L1_CURRENT_LOW': 'S32',
    'DEA_L2_CURRENT_LOW': 'S32',
    'DEA_L3_CURRENT_LOW': 'S32',
    'DEA_L1_VOLTAGE_LOW': 'S32',
    'DEA_L2_VOLTAGE_LOW': 'S32',
    'DEA_L3_VOLTAGE_LOW': 'S32',
    'DEA_TOTAL_ACTIVE_POWER_LOW':   'S32',
    'DEA_TOTAL_REACTIVE_POWER_LOW': 'S32',
    'DEA_POWER_FACTOR_LOW':         'S32',
    'DEA_FREQUENCY_LOW':            'S32',
    'DEA_STATUS_FLAG_LOW':          'S32',
}


FLOAT32_FIELDS = {
    k for k, v in DATA_TYPES.items() if v == 'FLOAT32'
}
STRING_CURRENT_MONITOR = True


# RTU batch read blocks — Ekos uses FC04 (Read Input Registers)
# Block 1: DER-AVM (0x03E8-0x03FD, 22 regs) — use FC03 for sim write-through
# Block 2: Status + PV + AC total + energy + per-phase + temp (0x0018-0x005B, 68 regs)
# Block 3: Cumulative MWh + fault maps (0x005E-0x0067, 10 regs)
# Block 4: String1/2 V/I/P (0x0068-0x0073, 12 regs)
READ_BLOCKS = [
    {'start': 0x03E8, 'count': 22, 'fc': 3},
    {'start': 0x0018, 'count': 68, 'fc': 4},
    {'start': 0x005E, 'count': 10, 'fc': 4},
    {'start': 0x0068, 'count': 12, 'fc': 4},
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
    'ac_power            ': 'ACTIVE_POWER',
    'cumulative_energy   ': 'CUMULATIVE_ENERGY',
    'alarm1              ': 'ERROR_CODE1',
    'string1_current'      : 'STRING1_CURRENT',
    'string2_current'      : 'STRING2_CURRENT',
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
    'ac_power            ': ('ACTIVE_POWER', 'power_to_W'),
    'pv_power            ': ('PV_POWER', 'power_to_W'),
    'inner_temp          ': ('INNER_TEMP', 'raw'),
    'power_factor        ': ('POWER_FACTOR', 'pf_raw'),
    'cumulative_energy   ': ('CUMULATIVE_ENERGY', 'energy_kwh_to_Wh'),
    'alarm1              ': ('ERROR_CODE1', 'raw'),
    'alarm2              ': ('ERROR_CODE2', 'raw'),
    'alarm3              ': ('ERROR_CODE3', 'raw'),
}


U32_WORD_ORDER = 'HL'  # Ekos: high word first (IEEE754 Float32 big-endian)
RTU_FC_CODE = 4        # Ekos uses FC04 (Read Input Registers)
