# -*- coding: utf-8 -*-
"""
Solis PV Inverter Modbus Register Map
Based on Solis RS485 MODBUS Protocol V19
FC: 03 (read holding), 04 (read input), 06 (write single)
Protocol name: solis
"""


class RegisterMap:
    # -------------------------------------------------------------------------
    # Device Information (FC03 Holding, 0x0BB8 - 0x0BBB)
    # -------------------------------------------------------------------------
    PRODUCT_MODEL        = 0x0BB8  # 3000: Product model (U16)
    DSP_SW_VERSION       = 0x0BB9  # 3001: DSP software version (U16)
    HMI_MAJOR_VERSION    = 0x0BBA  # 3002: HMI major version (U16)
    AC_OUTPUT_TYPE       = 0x0BBB  # 3003: AC output type (U16)

    # -------------------------------------------------------------------------
    # Power & Energy (0x0BBD - 0x0BC2)
    # -------------------------------------------------------------------------
    ACTIVE_POWER_H       = 0x0BBD  # 3005: Active power high word (S32)
    ACTIVE_POWER_L       = 0x0BBE  # 3006: Active power low word
    TOTAL_DC_POWER_H     = 0x0BBF  # 3007: Total DC output power high (U32, 1W)
    TOTAL_DC_POWER_L     = 0x0BC0  # 3008: Total DC output power low
    TOTAL_ENERGY_H       = 0x0BC1  # 3009: Total energy high word (U32, 1kWh)
    TOTAL_ENERGY_L       = 0x0BC2  # 3010: Total energy low word

    # -------------------------------------------------------------------------
    # Daily Energy (0x0BC7)
    # -------------------------------------------------------------------------
    DAILY_ENERGY         = 0x0BC7  # 3015: Energy today (U16, 0.1kWh)

    # -------------------------------------------------------------------------
    # PV / MPPT Channel Registers (0x0BCE - 0x0BD5)
    # -------------------------------------------------------------------------
    PV1_VOLTAGE          = 0x0BCE  # 3022: DC voltage 1 / MPPT1 voltage (U16, 0.1V)
    PV1_CURRENT          = 0x0BCF  # 3023: DC current 1 / MPPT1 current (U16, 0.1A)
    PV2_VOLTAGE          = 0x0BD0  # 3024: DC voltage 2 / MPPT2 voltage (U16, 0.1V)
    PV2_CURRENT          = 0x0BD1  # 3025: DC current 2 / MPPT2 current (U16, 0.1A)
    PV3_VOLTAGE          = 0x0BD2  # 3026: DC voltage 3 (U16, 0.1V)
    PV3_CURRENT          = 0x0BD3  # 3027: DC current 3 (U16, 0.1A)
    PV4_VOLTAGE          = 0x0BD4  # 3028: DC voltage 4 (U16, 0.1V)
    PV4_CURRENT          = 0x0BD5  # 3029: DC current 4 (U16, 0.1A)

    # -------------------------------------------------------------------------
    # Fault / Alarm (0x0BD6)
    # -------------------------------------------------------------------------
    ALARM_CODE           = 0x0BD6  # 3030: Alarm code data (U16)
    DC_BUS_VOLTAGE       = 0x0BD8  # 3032: DC busbar voltage (U16, 0.1V)

    # -------------------------------------------------------------------------
    # Grid / AC Phase Registers (0x0BDA - 0x0BDF)
    # -------------------------------------------------------------------------
    L1_VOLTAGE           = 0x0BDA  # 3034: AB line voltage / A phase voltage (U16, 0.1V)
    L2_VOLTAGE           = 0x0BDB  # 3035: BC line voltage / B phase voltage (U16, 0.1V)
    L3_VOLTAGE           = 0x0BDC  # 3036: CA line voltage / C phase voltage (U16, 0.1V)
    L1_CURRENT           = 0x0BDD  # 3037: A phase current (U16, 0.1A)
    L2_CURRENT           = 0x0BDE  # 3038: B phase current (U16, 0.1A)
    L3_CURRENT           = 0x0BDF  # 3039: C phase current (U16, 0.1A)

    # -------------------------------------------------------------------------
    # Inverter Status & Temperature (0x0BE2 - 0x0BE4)
    # -------------------------------------------------------------------------
    TEMPERATURE          = 0x0BE2  # 3042: Inverter temperature (S16, 0.1°C)
    FREQUENCY            = 0x0BE3  # 3043: Grid frequency (U16, 0.01Hz)
    INVERTER_STATUS      = 0x0BE4  # 3044: Inverter status (U16)

    # -------------------------------------------------------------------------
    # Control Registers (0x0BE9, 0x0BEC - 0x0BED, 0x0BFE)
    # -------------------------------------------------------------------------
    CONTROL_WORD         = 0x0BE9  # 3049: Inverter control word (FC06)
    POWER_FACTOR_SET     = 0x0BEC  # 3052: Power factor setting
    REACTIVE_POWER_PCT   = 0x0BED  # 3053: Reactive power value %

    # -------------------------------------------------------------------------
    # Serial Number (0x0BF4 - 0x0BF8)
    # -------------------------------------------------------------------------
    POWER_FACTOR_RT      = 0x0BF4  # 3060: Real-time power factor (S16, 0.001)
    SERIAL_NUMBER_1      = 0x0BF5  # 3061: SN word 1
    SERIAL_NUMBER_2      = 0x0BF6  # 3062: SN word 2
    SERIAL_NUMBER_3      = 0x0BF7  # 3063: SN word 3
    SERIAL_NUMBER_4      = 0x0BF8  # 3064: SN word 4

    # -------------------------------------------------------------------------
    # Power Limit / Additional Control (0x0BFE)
    # -------------------------------------------------------------------------
    POWER_LIMIT          = 0x0BFE  # 3070: Power limit value (holding, FC06)

    # -------------------------------------------------------------------------
    # Temperature 2 & Fault Codes (0x0C15 - 0x0C1A)
    # -------------------------------------------------------------------------
    TEMPERATURE2         = 0x0C15  # 3093: Inverter temperature 2 (S16, 0.1°C)
    FAULT_CODE1          = 0x0C18  # 3096: Fault Code 01
    FAULT_CODE2          = 0x0C19  # 3097: Fault Code 02
    FAULT_CODE3          = 0x0C1A  # 3098: Fault Code 03

    # -------------------------------------------------------------------------
    # Real-time Reactive Power (0x0E85)
    # -------------------------------------------------------------------------
    RT_REACTIVE_POWER    = 0x0E85  # 3717: Real-time reactive power

    # -------------------------------------------------------------------------
    # Phase Aliases (R/S/T = A/B/C = L1/L2/L3)
    # -------------------------------------------------------------------------
    R_VOLTAGE = L1_VOLTAGE
    S_VOLTAGE = L2_VOLTAGE
    T_VOLTAGE = L3_VOLTAGE
    R_CURRENT = L1_CURRENT
    S_CURRENT = L2_CURRENT
    T_CURRENT = L3_CURRENT

    R_PHASE_VOLTAGE = L1_VOLTAGE
    S_PHASE_VOLTAGE = L2_VOLTAGE
    T_PHASE_VOLTAGE = L3_VOLTAGE
    R_PHASE_CURRENT = L1_CURRENT
    S_PHASE_CURRENT = L2_CURRENT
    T_PHASE_CURRENT = L3_CURRENT

    # -------------------------------------------------------------------------
    # RTU Mandatory Aliases
    # -------------------------------------------------------------------------
    INVERTER_MODE    = INVERTER_STATUS
    AC_POWER         = ACTIVE_POWER_H   # S32 high word; handler combines with ACTIVE_POWER_L
    TOTAL_ENERGY     = TOTAL_ENERGY_H   # U32 high word (unit: 1 kWh — handler scales)
    INNER_TEMP       = TEMPERATURE
    MPPT1_VOLTAGE    = PV1_VOLTAGE
    MPPT1_CURRENT    = PV1_CURRENT
    MPPT2_VOLTAGE    = PV2_VOLTAGE
    MPPT2_CURRENT    = PV2_CURRENT
    ERROR_CODE1      = FAULT_CODE1

    # -------------------------------------------------------------------------
    # DER-AVM Aliases
    # -------------------------------------------------------------------------
    DER_POWER_FACTOR_SET   = 0x0BEC  # 3052 power factor setting
    DER_ACTION_MODE        = 0x0BE9  # 3049 control word
    DER_REACTIVE_POWER_PCT = 0x0BED  # 3053 reactive power %
    DER_ACTIVE_POWER_PCT   = 0x0BFE  # 3070 power limit value
    INVERTER_ON_OFF        = 0x0BE9  # 3049 control word


class InverterMode:
    """Solis inverter status codes (register 3044 / 0x0BE4).

    0x0000 No input  → INITIAL
    0x0001 Waiting   → STANDBY
    0x0002 Normal    → ON_GRID
    0x0003 Error     → FAULT
    0x0004 Check     → STANDBY (checking)
    """
    INITIAL  = 0x00
    STANDBY  = 0x01
    ON_GRID  = 0x02
    FAULT    = 0x05
    SHUTDOWN = 0x09

    @classmethod
    def to_string(cls, mode):
        _map = {
            0x00: "Initial",
            0x01: "Standby",
            0x02: "On-Grid",
            0x05: "Fault",
            0x09: "Shutdown",
        }
        return _map.get(mode, f"Unknown(0x{mode:02X})")


class DeviceType:
    RTU                = 0
    INVERTER           = 1
    ENVIRONMENT_SENSOR = 2
    POWER_METER        = 3
    PROTECTION_RELAY   = 4

    @classmethod
    def to_string(cls, dtype):
        return {
            0: "RTU",
            1: "Inverter",
            2: "Environment Sensor",
            3: "Power Meter",
            4: "Protection Relay",
        }.get(dtype, f"Unknown({dtype})")


class ControlMode:
    NONE    = "NONE"
    DER_AVM = "DER_AVM"


class SolisStatusConverter:
    """Map raw INVERTER_STATUS (3044) values to canonical InverterMode constants."""
    STATUS_MAP = {
        0x0000: InverterMode.INITIAL,   # No input
        0x0001: InverterMode.STANDBY,   # Waiting
        0x0002: InverterMode.ON_GRID,   # Normal
        0x0003: InverterMode.FAULT,     # Error / fault
        0x0004: InverterMode.STANDBY,   # Check (checking grid)
    }

    @classmethod
    def to_inverter_mode(cls, raw):
        return cls.STATUS_MAP.get(raw, InverterMode.INITIAL)


StatusConverter = SolisStatusConverter

# ---------------------------------------------------------------------------
# Scale factors
# ---------------------------------------------------------------------------
SCALE = {
    'voltage':            0.1,
    'current':            0.1,    # Solis current registers scale 0.1A
    'power':              0.1,
    'frequency':          0.01,
    'power_factor':       0.001,
    # DER-AVM read-back scales
    'dea_current':        0.1,
    'dea_voltage':        0.1,
    'dea_active_power':   0.1,
    'dea_reactive_power': 1,
    'dea_frequency':      0.1,
    # IV scan scales
    'iv_voltage':         0.1,
    'iv_current':         0.1,
}

# ---------------------------------------------------------------------------
# MPPT / String channel counts
# ---------------------------------------------------------------------------
MPPT_CHANNELS   = 2
STRING_CHANNELS = 0

# ---------------------------------------------------------------------------
# U32 / S32 helpers
# ---------------------------------------------------------------------------
def registers_to_u32(low, high):
    """Combine two 16-bit Modbus registers into an unsigned 32-bit integer."""
    return (high << 16) | low


def registers_to_s32(low, high):
    """Combine two 16-bit Modbus registers into a signed 32-bit integer."""
    v = (high << 16) | low
    return v - 0x100000000 if v >= 0x80000000 else v


STRING_CURRENT_MONITOR = False

# ---------------------------------------------------------------------------
# READ_BLOCKS
# Each entry: {'start': <hex addr>, 'count': <num regs>, 'fc': <function code>}
# fc=3 → FC03 Read Holding Registers
# ---------------------------------------------------------------------------
READ_BLOCKS = [
    # 0x0BB8-0x0BEB → 3000-3051: product info, power, energy, PV channels,
    #                              alarm, DC bus, grid phases, temp, status, control
    {'start': 0x0BB8, 'count': 55, 'fc': 3},
    # 0x0BF4-0x0BFF → 3060-3071: real-time PF, serial numbers, power limit
    {'start': 0x0BF4, 'count': 12, 'fc': 3},
    # 0x0C15-0x0C1E → 3093-3102: temp2, fault codes
    {'start': 0x0C15, 'count': 10, 'fc': 3},
]

# ---------------------------------------------------------------------------
# DATA_PARSER
# Maps logical RTU field names → RegisterMap attribute names
# ---------------------------------------------------------------------------
DATA_PARSER = {
    'mode':              'INVERTER_MODE',
    'r_voltage':         'R_PHASE_VOLTAGE',
    's_voltage':         'S_PHASE_VOLTAGE',
    't_voltage':         'T_PHASE_VOLTAGE',
    'r_current':         'R_PHASE_CURRENT',
    's_current':         'S_PHASE_CURRENT',
    't_current':         'T_PHASE_CURRENT',
    'frequency':         'FREQUENCY',
    'ac_power':          'AC_POWER',
    'temperature':       'INNER_TEMP',
    'cumulative_energy': 'TOTAL_ENERGY',
    'alarm1':            'ERROR_CODE1',
    'mppt1_voltage':     'MPPT1_VOLTAGE',
    'mppt1_current':     'MPPT1_CURRENT',
    'mppt2_voltage':     'MPPT2_VOLTAGE',
    'mppt2_current':     'MPPT2_CURRENT',
}
