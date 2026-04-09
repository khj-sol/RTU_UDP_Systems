# -*- coding: utf-8 -*-
"""Huawei SUN2000 50kW 3-phase inverter register map.

Based on Huawei-PV_SUN2000MC_Modbus_Interface_Definitions.pdf.
Configuration: MPPT=4, Strings=8 (2 strings per MPPT).

IMPORTANT: Huawei exposes ONLY per-string (PV1..PV8) V/I registers,
NOT per-MPPT registers. MPPT voltage/current is derived by
rtu_program/modbus_handler.py via STRINGS_PER_MPPT aggregation
(voltage = average of strings on same MPPT, current = sum of strings).

Huawei uses FC03 (Read Holding Registers).
Register numbers per PDF are 1-based (e.g. 32016) which maps to the
Modbus address 32016 (0x7D10).
"""


class RegisterMap:
    """Huawei SUN2000 register addresses per PDF."""

    # =========================================================================
    # Alarms (32008-32013)  Bitfield16
    # =========================================================================
    ALARM1                                   = 0x7D08  # 32008
    ALARM2                                   = 0x7D09  # 32009
    ALARM3                                   = 0x7D0A  # 32010
    ALARM4                                   = 0x7D0B  # 32011
    ALARM5                                   = 0x7D0C  # 32012
    ALARM6                                   = 0x7D0D  # 32013

    # =========================================================================
    # PV strings (32016-32031)  I16, V gain 10, I gain 100
    # PDF: "PV{N} voltage/current" — Huawei has only per-string regs.
    # =========================================================================
    PV1_VOLTAGE                              = 0x7D10  # 32016
    PV1_CURRENT                              = 0x7D11  # 32017
    PV2_VOLTAGE                              = 0x7D12  # 32018
    PV2_CURRENT                              = 0x7D13  # 32019
    PV3_VOLTAGE                              = 0x7D14  # 32020
    PV3_CURRENT                              = 0x7D15  # 32021
    PV4_VOLTAGE                              = 0x7D16  # 32022
    PV4_CURRENT                              = 0x7D17  # 32023
    PV5_VOLTAGE                              = 0x7D18  # 32024
    PV5_CURRENT                              = 0x7D19  # 32025
    PV6_VOLTAGE                              = 0x7D1A  # 32026
    PV6_CURRENT                              = 0x7D1B  # 32027
    PV7_VOLTAGE                              = 0x7D1C  # 32028
    PV7_CURRENT                              = 0x7D1D  # 32029
    PV8_VOLTAGE                              = 0x7D1E  # 32030
    PV8_CURRENT                              = 0x7D1F  # 32031

    # STRING{N} aliases for RTU compat (8 strings = 4 MPPT × 2 strings/MPPT)
    STRING1_VOLTAGE                          = PV1_VOLTAGE
    STRING1_CURRENT                          = PV1_CURRENT
    STRING2_VOLTAGE                          = PV2_VOLTAGE
    STRING2_CURRENT                          = PV2_CURRENT
    STRING3_VOLTAGE                          = PV3_VOLTAGE
    STRING3_CURRENT                          = PV3_CURRENT
    STRING4_VOLTAGE                          = PV4_VOLTAGE
    STRING4_CURRENT                          = PV4_CURRENT
    STRING5_VOLTAGE                          = PV5_VOLTAGE
    STRING5_CURRENT                          = PV5_CURRENT
    STRING6_VOLTAGE                          = PV6_VOLTAGE
    STRING6_CURRENT                          = PV6_CURRENT
    STRING7_VOLTAGE                          = PV7_VOLTAGE
    STRING7_CURRENT                          = PV7_CURRENT
    STRING8_VOLTAGE                          = PV8_VOLTAGE
    STRING8_CURRENT                          = PV8_CURRENT

    # =========================================================================
    # Input power (32064-32065)  I32 kW gain 1000
    # =========================================================================
    DC_POWER                                 = 0x7D40  # 32064
    DC_POWER_HIGH                            = 0x7D41
    PV_POWER                                 = DC_POWER
    PV_POWER_HIGH                            = DC_POWER_HIGH

    # =========================================================================
    # Grid voltages (32066-32071)  U16 V gain 10
    # =========================================================================
    GRID_AB_VOLTAGE                          = 0x7D42  # 32066
    GRID_BC_VOLTAGE                          = 0x7D43  # 32067
    GRID_CA_VOLTAGE                          = 0x7D44  # 32068
    PHASE_A_VOLTAGE                          = 0x7D45  # 32069
    PHASE_B_VOLTAGE                          = 0x7D46  # 32070
    PHASE_C_VOLTAGE                          = 0x7D47  # 32071

    # Grid phase currents (32072-32077)  I32 A gain 1000
    # Note: H01 expects 0.01A integer, so we read the low word only (U16 path)
    #       OR use S32 with scale conversion in handler. Huawei uses I32 × 1000,
    #       so raw / 10 = 0.01A units. We expose low+high pair and let the
    #       RTU convert. Using S16 view of low word would lose precision, so
    #       we use the S32 pair and the converter divides by 10 in H01 map.
    PHASE_A_CURRENT                          = 0x7D48  # 32072 (low)
    PHASE_A_CURRENT_HIGH                     = 0x7D49  # 32073
    PHASE_B_CURRENT                          = 0x7D4A  # 32074
    PHASE_B_CURRENT_HIGH                     = 0x7D4B  # 32075
    PHASE_C_CURRENT                          = 0x7D4C  # 32076
    PHASE_C_CURRENT_HIGH                     = 0x7D4D  # 32077

    # =========================================================================
    # Active / reactive power / PF / freq (32080-32087)
    # =========================================================================
    ACTIVE_POWER                             = 0x7D50  # 32080 I32 kW gain 1000
    ACTIVE_POWER_HIGH                        = 0x7D51
    REACTIVE_POWER                           = 0x7D52  # 32082 I32 kVar gain 1000
    REACTIVE_POWER_HIGH                      = 0x7D53
    POWER_FACTOR                             = 0x7D54  # 32084 I16 gain 1000
    FREQUENCY                                = 0x7D55  # 32085 U16 Hz gain 100
    INVERTER_EFFICIENCY                      = 0x7D56  # 32086 U16 % gain 100
    INTERNAL_TEMP                            = 0x7D57  # 32087 I16 °C gain 10
    INNER_TEMP                               = INTERNAL_TEMP
    INSULATION_IMPEDANCE                     = 0x7D58  # 32088 U16 MΩ gain 100

    # =========================================================================
    # Status + fault (32089-32090)
    # =========================================================================
    DEVICE_STATUS                            = 0x7D59  # 32089 U16
    INVERTER_MODE                            = DEVICE_STATUS
    FAULT_CODE                               = 0x7D5A  # 32090 U16

    # =========================================================================
    # Energy (32106-32109)
    # =========================================================================
    ACCUMULATED_ENERGY_YIELD                 = 0x7D6A  # 32106 U32 kWh gain 10
    ACCUMULATED_ENERGY_YIELD_HIGH            = 0x7D6B
    CUMULATIVE_ENERGY                        = ACCUMULATED_ENERGY_YIELD
    CUMULATIVE_ENERGY_HIGH                   = ACCUMULATED_ENERGY_YIELD_HIGH
    TOTAL_ENERGY                             = ACCUMULATED_ENERGY_YIELD

    TOTAL_DC_INPUT_POWER                     = 0x7D6C  # 32108 U32 kWh gain 10
    TOTAL_DC_INPUT_POWER_HIGH                = 0x7D6D

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

    # H01 aliases
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
    AC_POWER                                 = ACTIVE_POWER
    AC_POWER_HIGH                            = ACTIVE_POWER_HIGH
    PV_VOLTAGE                               = PV1_VOLTAGE

    # Error code aliases
    ERROR_CODE1                              = ALARM1
    ERROR_CODE2                              = ALARM2
    ERROR_CODE3                              = ALARM3


    # Device info (FC03) — Huawei SUN2000MC Modbus Interface Definitions
    # Model: ASCII STRING 15 regs = 30 bytes at 0x7530 (30000)
    # Serial: ASCII STRING 10 regs = 20 bytes at 0x753F (30015)
    DEVICE_MODEL                             = 0x7530
    DEVICE_MODEL_SIZE                        = 15
    DEVICE_SERIAL_NUMBER                     = 0x753F
    DEVICE_SERIAL_NUMBER_SIZE                = 10


class InverterMode:
    """Huawei device status codes (reg 32089) — mapped to Solarize modes.

    PDF Table:
      0x0000-0x0003 = Standby   → STANDBY
      0x0100        = Starting  → INITIAL
      0x0200-0x0202 = On-grid   → ON_GRID
      0x0300-0x0307 = Shutdown  → SHUTDOWN / FAULT
      0x0500+       = misc      → STANDBY
    """
    INITIAL  = 0x0100
    STANDBY  = 0x0000
    ON_GRID  = 0x0200
    FAULT    = 0x0300
    SHUTDOWN = 0x0301

    @classmethod
    def to_string(cls, v):
        mapping = {
            0x0000: 'STANDBY_INIT',
            0x0001: 'STANDBY_INSULATION',
            0x0002: 'STANDBY_IRRADIATION',
            0x0003: 'STANDBY_GRID_DETECT',
            0x0100: 'STARTING',
            0x0200: 'ON_GRID',
            0x0201: 'ON_GRID_POWER_LIMITED',
            0x0202: 'ON_GRID_SELF_DERATING',
            0x0300: 'SHUTDOWN_FAULT',
            0x0301: 'SHUTDOWN_COMMAND',
            0x0302: 'SHUTDOWN_OVGR',
            0x0303: 'SHUTDOWN_COMM_DISCONNECT',
            0x0304: 'SHUTDOWN_POWER_LIMITED',
            0x0305: 'SHUTDOWN_MANUAL',
            0x0307: 'SHUTDOWN_RAPID_CUTOFF',
        }
        return mapping.get(v, f'UNKNOWN({v:#06x})')


class HuaweiStatusConverter:
    @staticmethod
    def to_inverter_mode(raw):
        # Map Huawei 16-bit status to canonical InverterMode values
        if raw is None:
            return InverterMode.STANDBY
        if 0x0000 <= raw <= 0x00FF:
            return InverterMode.STANDBY
        if raw == 0x0100:
            return InverterMode.INITIAL
        if 0x0200 <= raw <= 0x02FF:
            return InverterMode.ON_GRID
        if raw == 0x0300:
            return InverterMode.FAULT
        if 0x0301 <= raw <= 0x03FF:
            return InverterMode.SHUTDOWN
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
    """Alarm 1 bitfield (see Huawei Alarm Mapping sheet)."""
    BITS = {}


class ErrorCode2:
    BITS = {}


class ErrorCode3:
    BITS = {}


SCALE = {
    # Simulator + RTU reader must agree on raw→physical mapping.
    # Convention (matches Kstar/Solarize): current raw in 0.01A, voltage raw
    # in 0.1V, frequency raw in 0.01Hz, power raw in 0.1W.
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


# Channel configuration — 50kW SUN2000: 4 MPPT, 8 strings (2/MPPT)
MPPT_CHANNELS = 4
STRING_CHANNELS = 8
STRINGS_PER_MPPT = 2   # triggers MPPT-from-strings aggregation in modbus_handler


def registers_to_u32(low, high):
    return (high << 16) | low


def registers_to_s32(low, high):
    value = (high << 16) | low
    if value >= 0x80000000:
        value -= 0x100000000
    return value


DATA_TYPES = {
    # PV strings: I16 (signed, scale 0.1V / 0.01A)
    'PV1_VOLTAGE':  'S16',
    'PV1_CURRENT':  'S16',
    'PV2_VOLTAGE':  'S16',
    'PV2_CURRENT':  'S16',
    'PV3_VOLTAGE':  'S16',
    'PV3_CURRENT':  'S16',
    'PV4_VOLTAGE':  'S16',
    'PV4_CURRENT':  'S16',
    'PV5_VOLTAGE':  'S16',
    'PV5_CURRENT':  'S16',
    'PV6_VOLTAGE':  'S16',
    'PV6_CURRENT':  'S16',
    'PV7_VOLTAGE':  'S16',
    'PV7_CURRENT':  'S16',
    'PV8_VOLTAGE':  'S16',
    'PV8_CURRENT':  'S16',
    'STRING1_VOLTAGE': 'S16',
    'STRING1_CURRENT': 'S16',
    'STRING2_VOLTAGE': 'S16',
    'STRING2_CURRENT': 'S16',
    'STRING3_VOLTAGE': 'S16',
    'STRING3_CURRENT': 'S16',
    'STRING4_VOLTAGE': 'S16',
    'STRING4_CURRENT': 'S16',
    'STRING5_VOLTAGE': 'S16',
    'STRING5_CURRENT': 'S16',
    'STRING6_VOLTAGE': 'S16',
    'STRING6_CURRENT': 'S16',
    'STRING7_VOLTAGE': 'S16',
    'STRING7_CURRENT': 'S16',
    'STRING8_VOLTAGE': 'S16',
    'STRING8_CURRENT': 'S16',
    # Grid — phase voltage U16, phase current S32, power S32
    'PHASE_A_VOLTAGE': 'U16',
    'PHASE_B_VOLTAGE': 'U16',
    'PHASE_C_VOLTAGE': 'U16',
    'L1_VOLTAGE':      'U16',
    'L2_VOLTAGE':      'U16',
    'L3_VOLTAGE':      'U16',
    'PHASE_A_CURRENT': 'S32',
    'PHASE_B_CURRENT': 'S32',
    'PHASE_C_CURRENT': 'S32',
    'L1_CURRENT':      'S32',
    'L2_CURRENT':      'S32',
    'L3_CURRENT':      'S32',
    'ACTIVE_POWER':    'S32',
    'AC_POWER':        'S32',
    'REACTIVE_POWER':  'S32',
    'DC_POWER':        'S32',
    'PV_POWER':        'S32',
    'POWER_FACTOR':    'S16',
    'FREQUENCY':       'U16',
    'INTERNAL_TEMP':   'S16',
    'INNER_TEMP':      'S16',
    'DEVICE_STATUS':   'U16',
    'INVERTER_MODE':   'U16',
    'FAULT_CODE':      'U16',
    'ALARM1':          'U16',
    'ALARM2':          'U16',
    'ALARM3':          'U16',
    'ERROR_CODE1':     'U16',
    'ERROR_CODE2':     'U16',
    'ERROR_CODE3':     'U16',
    'ACCUMULATED_ENERGY_YIELD': 'U32',
    'CUMULATIVE_ENERGY':        'U32',
    'TOTAL_DC_INPUT_POWER':     'U32',
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


FLOAT32_FIELDS: set = set()
STRING_CURRENT_MONITOR = True


# RTU batch read blocks — Huawei uses FC03 (Read Holding Registers)
# Block 1: DER-AVM (0x03E8-0x03FD, 22 regs) — FC03
# Block 2: Alarms (0x7D08-0x7D0D, 6 regs)
# Block 3: PV strings 1-8 (0x7D10-0x7D1F, 16 regs)
# Block 4: DC power + grid V + phase I + active/reactive + PF + freq + temp
#          + status + fault (0x7D40-0x7D5A, 27 regs)
# Block 5: Accumulated energy + DC energy (0x7D6A-0x7D6D, 4 regs)
READ_BLOCKS = [
    {'start': 0x03E8, 'count': 22, 'fc': 3},
    {'start': 0x7D08, 'count':  6, 'fc': 3},
    {'start': 0x7D10, 'count': 16, 'fc': 3},
    {'start': 0x7D40, 'count': 27, 'fc': 3},
    {'start': 0x7D6A, 'count':  4, 'fc': 3},
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


# H01 field → (attr, converter) mapping
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


U32_WORD_ORDER = 'HL'  # Huawei: high word first
RTU_FC_CODE = 3        # Huawei uses FC03 (Read Holding Registers)
