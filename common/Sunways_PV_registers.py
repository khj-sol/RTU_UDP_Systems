# -*- coding: utf-8 -*-
"""
Sunways PV Inverter Modbus Register Map
Based on Sunways PV Inverter Modbus RTU Protocol v00.07
FC: 03 (read), 06 (write single), 16 (write multiple)
Protocol name: sunways
"""


class RegisterMap:
    # -------------------------------------------------------------------------
    # Device Information (0x2710 - 0x271F)
    # -------------------------------------------------------------------------
    SERIAL_NUMBER       = 0x2710  # 10000: Serial number, 8 words (STR)
    MODEL_INFO          = 0x2718  # 10008: Model information (U16)
    OUTPUT_MODE         = 0x2719  # 10009: Output mode (0=3-phase 4-wire)
    PROTOCOL_VERSION    = 0x271A  # 10010: Communication protocol version

    # -------------------------------------------------------------------------
    # Time Registers (0x2774 - 0x2776)
    # -------------------------------------------------------------------------
    TIME_YEAR_MONTH     = 0x2774  # 10100: Time year/month
    TIME_DAY_HOUR       = 0x2775  # 10101: Time day/hour
    TIME_MIN_SEC        = 0x2776  # 10102: Time minute/second

    # -------------------------------------------------------------------------
    # Status & Fault Registers (0x2778 - 0x278B)
    # -------------------------------------------------------------------------
    SAFETY_STANDARD     = 0x2778  # 10104: Safety standard/area code
    WORKING_STATE       = 0x2779  # 10105: Working state of inverter
    FAULT_FLAG1_L       = 0x2780  # 10112: Fault FLAG1 low word (U32)
    FAULT_FLAG1_H       = 0x2781  # 10113: Fault FLAG1 high word
    FAULT_FLAG2_L       = 0x2782  # 10114: Fault FLAG2 low word (U32)
    FAULT_FLAG2_H       = 0x2783  # 10115: Fault FLAG2 high word
    FAULT_FLAG3_L       = 0x2788  # 10120: Fault FLAG3 low word (U32)
    FAULT_FLAG3_H       = 0x2789  # 10121: Fault FLAG3 high word

    # -------------------------------------------------------------------------
    # Grid / AC Monitoring (0x2B06 - 0x2B21)
    # -------------------------------------------------------------------------
    GRID_LINE_AB_VOLTAGE = 0x2B06  # 11006: Grid LINE AB voltage (U16, 0.1V)
    GRID_LINE_BC_VOLTAGE = 0x2B07  # 11007: Grid LINE BC voltage (U16, 0.1V)
    GRID_LINE_CA_VOLTAGE = 0x2B08  # 11008: Grid LINE CA voltage (U16, 0.1V)
    L1_VOLTAGE           = 0x2B09  # 11009: Grid A phase voltage (U16, 0.1V)
    L1_CURRENT           = 0x2B0A  # 11010: Grid A phase current (U16, 0.1A)
    L2_VOLTAGE           = 0x2B0B  # 11011: Grid B phase voltage (U16, 0.1V)
    L2_CURRENT           = 0x2B0C  # 11012: Grid B phase current (U16, 0.1A)
    L3_VOLTAGE           = 0x2B0D  # 11013: Grid C phase voltage (U16, 0.1V)
    L3_CURRENT           = 0x2B0E  # 11014: Grid C phase current (U16, 0.1A)
    FREQUENCY            = 0x2B0F  # 11015: Grid frequency (U16, 0.01Hz)

    P_AC_L               = 0x2B10  # 11016: AC power low word  (U32, 0.001kW → W via *1)
    P_AC_H               = 0x2B11  # 11017: AC power high word
    DAILY_ENERGY_L       = 0x2B12  # 11018: Daily generating capacity low (U32, 0.1kWh)
    DAILY_ENERGY_H       = 0x2B13  # 11019: Daily generating capacity high
    TOTAL_ENERGY_L       = 0x2B14  # 11020: Total generation low (U32, 0.1kWh)
    TOTAL_ENERGY_H       = 0x2B15  # 11021: Total generation high
    TOTAL_TIME_L         = 0x2B16  # 11022: Total generating time low (U32)
    TOTAL_TIME_H         = 0x2B17  # 11023: Total generating time high
    APPARENT_POWER_L     = 0x2B18  # 11024: Output apparent power low (U32, 0.001kVA)
    APPARENT_POWER_H     = 0x2B19  # 11025: Output apparent power high
    REACTIVE_POWER_L     = 0x2B1A  # 11026: Output reactive power low (I32, 0.001kVAr)
    REACTIVE_POWER_H     = 0x2B1B  # 11027: Output reactive power high
    TOTAL_INPUT_POWER_L  = 0x2B1C  # 11028: Total power input low (U32, 0.001kW)
    TOTAL_INPUT_POWER_H  = 0x2B1D  # 11029: Total power input high
    POWER_FACTOR         = 0x2B1E  # 11030: Power factor (I16, 0.001)
    EFFICIENCY           = 0x2B1F  # 11031: Efficiency (U16, 0.1%)
    TEMPERATURE1         = 0x2B20  # 11032: Temperature 1 (I16, 0.1°C)
    TEMPERATURE2         = 0x2B21  # 11033: Temperature 2 (I16, 0.1°C)

    # -------------------------------------------------------------------------
    # Bus Voltage (0x2B24 - 0x2B25)
    # -------------------------------------------------------------------------
    BUS_VOLTAGE          = 0x2B24  # 11036: BUS Voltage (U16)
    NBS_VOLTAGE          = 0x2B25  # 11037: NBS Voltage (U16)

    # -------------------------------------------------------------------------
    # PV Channel Registers (0x2B26 onwards)
    # -------------------------------------------------------------------------
    PV1_VOLTAGE          = 0x2B26  # 11038: PV1 voltage (U16, 0.1V)
    PV1_CURRENT          = 0x2B27  # 11039: PV1 current (U16, 0.1A)
    PV2_VOLTAGE          = 0x2B28  # 11040: PV2 voltage (U16, 0.1V)
    PV2_CURRENT          = 0x2B29  # 11041: PV2 current (U16, 0.1A)
    PV3_VOLTAGE          = 0x2B2A  # 11042: PV3 voltage (U16, 0.1V)
    PV3_CURRENT          = 0x2B2B  # 11043: PV3 current (U16, 0.1A)
    PV4_VOLTAGE          = 0x2B2C  # 11044: PV4 voltage (U16, 0.1V)
    PV4_CURRENT          = 0x2B2D  # 11045: PV4 current (U16, 0.1A)
    PV5_VOLTAGE          = 0x2B2E  # 11046: PV5 voltage (U16, 0.1V)
    PV5_CURRENT          = 0x2B2F  # 11047: PV5 current (U16, 0.1A)
    PV6_VOLTAGE          = 0x2B30  # 11048: PV6 voltage (U16, 0.1V)
    PV6_CURRENT          = 0x2B31  # 11049: PV6 current (U16, 0.1A)
    PV7_VOLTAGE          = 0x2B32  # 11050: PV7 voltage (U16, 0.1V)
    PV7_CURRENT          = 0x2B33  # 11051: PV7 current (U16, 0.1A)
    PV8_VOLTAGE          = 0x2B34  # 11052: PV8 voltage (U16, 0.1V)
    PV8_CURRENT          = 0x2B35  # 11053: PV8 current (U16, 0.1A)

    # -------------------------------------------------------------------------
    # Control Registers
    # -------------------------------------------------------------------------
    DEVICE_RTC           = 0x4E20  # 20000: Device RTC clock
    SWITCH_ON_OFF        = 0x61B0  # 25008: Switch on/off (BIT0=1 on, BIT0=0 off)
    ACTIVE_POWER_LIMIT_L = 0x6218  # 25112: Active power limit low word (U32, W)
    ACTIVE_POWER_LIMIT_H = 0x6219  # 25113: Active power limit high word
    ACTIVE_POWER_PCT     = 0x621A  # 25114: Active power limit percentage (U16, 0.001, 0-1000)
    REACTIVE_POWER_LIM_L = 0x621C  # 25116: Reactive power limit low word (I32, W)
    REACTIVE_POWER_LIM_H = 0x621D  # 25117: Reactive power limit high word
    REACTIVE_POWER_PCT   = 0x621E  # 25118: Reactive power limit percentage (I16, 0.001, -600~+600)
    PF_SETTINGS          = 0x6220  # 25120: PF Settings (I16, 0.001)

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
    INVERTER_MODE    = WORKING_STATE
    AC_POWER         = P_AC_L           # U32 low word; handler combines with P_AC_H
    TOTAL_ENERGY     = TOTAL_ENERGY_L   # U32 low word
    INNER_TEMP       = TEMPERATURE1
    MPPT1_VOLTAGE    = PV1_VOLTAGE
    MPPT1_CURRENT    = PV1_CURRENT
    MPPT2_VOLTAGE    = PV2_VOLTAGE
    MPPT2_CURRENT    = PV2_CURRENT
    ERROR_CODE1      = FAULT_FLAG1_L

    # -------------------------------------------------------------------------
    # DER-AVM Aliases
    # -------------------------------------------------------------------------
    DER_POWER_FACTOR_SET  = 0x6220  # 25120 PF Settings
    DER_ACTION_MODE       = 0x61B0  # 25008 on/off (mode control)
    DER_REACTIVE_POWER_PCT = 0x621E  # 25118 reactive power percentage
    DER_ACTIVE_POWER_PCT  = 0x621A  # 25114 active power percentage
    INVERTER_ON_OFF       = 0x61B0  # 25008


class InverterMode:
    """Sunways working state codes (register 10105 / 0x2779).

    0 → Standby/Waiting, 1 → On-Grid normal, 2 → Fault
    Mapped to the five canonical RTU states.
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


class SunwaysStatusConverter:
    """Map raw WORKING_STATE (10105) values to canonical InverterMode constants."""
    STATUS_MAP = {
        0: InverterMode.STANDBY,   # 0 = Standby / Waiting
        1: InverterMode.ON_GRID,   # 1 = Normal on-grid
        2: InverterMode.FAULT,     # 2 = Fault
    }

    @classmethod
    def to_inverter_mode(cls, raw):
        return cls.STATUS_MAP.get(raw, InverterMode.INITIAL)


StatusConverter = SunwaysStatusConverter

# ---------------------------------------------------------------------------
# Scale factors
# ---------------------------------------------------------------------------
SCALE = {
    'voltage':            0.1,
    'current':            0.01,
    'power':              0.1,    # AC power raw unit is 0.001 kW → *1 gives W; stored *10 → 0.1W
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
# ---------------------------------------------------------------------------
READ_BLOCKS = [
    # 0x2778-0x2781 → 10104-10113: safety std, working state, fault flag1(U32)
    {'start': 0x2778, 'count': 10, 'fc': 3},
    # 0x2B06-0x2B25 → 11006-11037: grid voltages, currents, frequency, power, temp, bus
    {'start': 0x2B06, 'count': 32, 'fc': 3},
    # 0x2B26-0x2B39 → 11038-11057: PV1-PV8 voltage/current
    {'start': 0x2B26, 'count': 20, 'fc': 3},
]

# ---------------------------------------------------------------------------
# DATA_PARSER
# Maps logical RTU field names → RegisterMap attribute names
# ---------------------------------------------------------------------------
DATA_PARSER = {
    'mode':             'INVERTER_MODE',
    'r_voltage':        'R_PHASE_VOLTAGE',
    's_voltage':        'S_PHASE_VOLTAGE',
    't_voltage':        'T_PHASE_VOLTAGE',
    'r_current':        'R_PHASE_CURRENT',
    's_current':        'S_PHASE_CURRENT',
    't_current':        'T_PHASE_CURRENT',
    'frequency':        'FREQUENCY',
    'ac_power':         'AC_POWER',
    'temperature':      'INNER_TEMP',
    'cumulative_energy':'TOTAL_ENERGY',
    'alarm1':           'ERROR_CODE1',
    'mppt1_voltage':    'MPPT1_VOLTAGE',
    'mppt1_current':    'MPPT1_CURRENT',
    'mppt2_voltage':    'MPPT2_VOLTAGE',
    'mppt2_current':    'MPPT2_CURRENT',
}
