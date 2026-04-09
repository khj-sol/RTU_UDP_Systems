# -*- coding: utf-8 -*-
"""Kstar 60kW 3-phase inverter register map.

Based on Kstar-PV_KSG1250K_Modbus_Protocol_v35 PDF.
Configuration: MPPT=3, String=9 (3 strings per MPPT) for KSG-60K.

Register range: PV inputs at 3000-3011 (0x0BB8-0x0BC3),
grid at 3014-3024 (0x0BC6-0x0BD0), string currents at 3064-3074.

IMPORTANT: Kstar uses FC04 (Read Input Registers), not FC03.
"""


class RegisterMap:
    """Kstar KSG register addresses per PDF v35."""

    # =========================================================================
    # PV inputs (register 3000-3011 = 0x0BB8-0x0BC3)
    # PDF: "PV{N} input voltage/current/power"
    # =========================================================================
    PV1_INPUT_VOLTAGE                        = 0x0BB8  # U16, 0.1V (reg 3000)
    PV2_INPUT_VOLTAGE                        = 0x0BB9  # U16, 0.1V
    PV3_INPUT_VOLTAGE                        = 0x0BBA  # U16, 0.1V
    PV1_INPUT_CURRENT                        = 0x0BBB  # U16, 0.01A
    PV2_INPUT_CURRENT                        = 0x0BBC  # U16, 0.01A
    PV3_INPUT_CURRENT                        = 0x0BBD  # U16, 0.01A
    PV1_INPUT_POWER                          = 0x0BBE  # S32, 1W (2 regs)
    PV1_INPUT_POWER_HIGH                     = 0x0BBF
    PV2_INPUT_POWER                          = 0x0BC0  # S32, 1W
    PV2_INPUT_POWER_HIGH                     = 0x0BC1
    PV3_INPUT_POWER                          = 0x0BC2  # S32, 1W
    PV3_INPUT_POWER_HIGH                     = 0x0BC3

    # MPPT{N} aliases for RTU compat (same as PV{N})
    MPPT1_VOLTAGE                            = PV1_INPUT_VOLTAGE
    MPPT1_CURRENT                            = PV1_INPUT_CURRENT
    MPPT1_POWER                              = PV1_INPUT_POWER
    MPPT1_POWER_HIGH                         = PV1_INPUT_POWER_HIGH
    MPPT2_VOLTAGE                            = PV2_INPUT_VOLTAGE
    MPPT2_CURRENT                            = PV2_INPUT_CURRENT
    MPPT2_POWER                              = PV2_INPUT_POWER
    MPPT2_POWER_HIGH                         = PV2_INPUT_POWER_HIGH
    MPPT3_VOLTAGE                            = PV3_INPUT_VOLTAGE
    MPPT3_CURRENT                            = PV3_INPUT_CURRENT
    MPPT3_POWER                              = PV3_INPUT_POWER
    MPPT3_POWER_HIGH                         = PV3_INPUT_POWER_HIGH

    # =========================================================================
    # PBUS/NBUS voltage (3012-3013)
    # =========================================================================
    PBUS_VOLTAGE                             = 0x0BC4  # U16, 0.1V
    NBUS_VOLTAGE                             = 0x0BC5  # U16, 0.1V

    # =========================================================================
    # Grid AC voltages/currents/frequency (3014-3024)
    # PDF: RS/ST/TR phase grid voltage, R/S/T phase current
    # =========================================================================
    L1_VOLTAGE                               = 0x0BC6  # U16, 0.1V (RS-phase)
    L2_VOLTAGE                               = 0x0BC7  # U16, 0.1V (ST-phase)
    L3_VOLTAGE                               = 0x0BC8  # U16, 0.1V (TR-phase)
    FREQUENCY                                = 0x0BC9  # U16, 0.01Hz (RS-phase)
    L2_FREQUENCY                             = 0x0BCA  # U16, 0.01Hz (ST-phase)
    L3_FREQUENCY                             = 0x0BCB  # U16, 0.01Hz (TR-phase)
    L1_CURRENT                               = 0x0BCC  # U16, 0.01A (R-phase)
    L2_CURRENT                               = 0x0BCD  # U16, 0.01A (S-phase)
    L3_CURRENT                               = 0x0BCE  # U16, 0.01A (T-phase)
    AC_POWER                                 = 0x0BCF  # S32, 1W (grid-tied)
    AC_POWER_HIGH                            = 0x0BD0

    # =========================================================================
    # Temperature & status (3025-3030)
    # =========================================================================
    INNER_TEMP                               = 0x0BD1  # S16, 0.1°C (Radiator)
    MODULE_TEMPERATURE                       = 0x0BD2  # S16, 0.1°C
    DSP_ALARM_CODE                           = 0x0BD3  # U16 (Table 3.1.2)
    DSP_ERROR_CODE                           = 0x0BD4  # U32 (Table 3.1.3)
    DSP_ERROR_CODE_HIGH                      = 0x0BD5
    INVERTER_MODE                            = 0x0BD6  # U16, operating mode (low byte)

    # =========================================================================
    # Energy / power factor (3038-3056)
    # =========================================================================
    CUMULATIVE_ENERGY                        = 0x0BDE  # U32, 0.1kWh (reg 3038)
    CUMULATIVE_ENERGY_HIGH                   = 0x0BDF
    TOTAL_ENERGY                             = CUMULATIVE_ENERGY  # alias
    POWER_FACTOR                             = 0x0BF0  # U16 (reg 3056)

    # =========================================================================
    # String currents (3064-3075 = 0x0BF8-0x0C03)
    # PV1 strings: 0x0BF8-0x0BFB (4 slots, use 3)
    # PV2 strings: 0x0BFC-0x0BFF (4 slots, use 3)
    # PV3 strings: 0x0C00-0x0C03 (4 slots, use 3)
    # =========================================================================
    PV1_STRING_CURRENT_1                     = 0x0BF8  # S16, 0.01A
    PV1_STRING_CURRENT_2                     = 0x0BF9
    PV1_STRING_CURRENT_3                     = 0x0BFA
    PV1_STRING_CURRENT_4                     = 0x0BFB  # unused for 9-string config
    PV2_STRING_CURRENT_1                     = 0x0BFC
    PV2_STRING_CURRENT_2                     = 0x0BFD
    PV2_STRING_CURRENT_3                     = 0x0BFE
    PV2_STRING_CURRENT_4                     = 0x0BFF
    PV3_STRING_CURRENT_1                     = 0x0C00
    PV3_STRING_CURRENT_2                     = 0x0C01
    PV3_STRING_CURRENT_3                     = 0x0C02
    PV3_STRING_CURRENT_4                     = 0x0C03

    # STRING{N} aliases — 9 strings mapping:
    # STR1-3 = PV1 str 1-3, STR4-6 = PV2 str 1-3, STR7-9 = PV3 str 1-3
    STRING1_CURRENT                          = PV1_STRING_CURRENT_1
    STRING2_CURRENT                          = PV1_STRING_CURRENT_2
    STRING3_CURRENT                          = PV1_STRING_CURRENT_3
    STRING4_CURRENT                          = PV2_STRING_CURRENT_1
    STRING5_CURRENT                          = PV2_STRING_CURRENT_2
    STRING6_CURRENT                          = PV2_STRING_CURRENT_3
    STRING7_CURRENT                          = PV3_STRING_CURRENT_1
    STRING8_CURRENT                          = PV3_STRING_CURRENT_2
    STRING9_CURRENT                          = PV3_STRING_CURRENT_3

    # String voltage — Kstar has no per-string voltage, alias to MPPT
    STRING1_VOLTAGE                          = MPPT1_VOLTAGE
    STRING2_VOLTAGE                          = MPPT1_VOLTAGE
    STRING3_VOLTAGE                          = MPPT1_VOLTAGE
    STRING4_VOLTAGE                          = MPPT2_VOLTAGE
    STRING5_VOLTAGE                          = MPPT2_VOLTAGE
    STRING6_VOLTAGE                          = MPPT2_VOLTAGE
    STRING7_VOLTAGE                          = MPPT3_VOLTAGE
    STRING8_VOLTAGE                          = MPPT3_VOLTAGE
    STRING9_VOLTAGE                          = MPPT3_VOLTAGE

    # =========================================================================
    # IV Scan (3126 status + 4035 command + 5000-5599 data)
    # =========================================================================
    IV_SCAN_STATUS                           = 0x0C36  # U16 (reg 3126)
    IV_SCAN_COMMAND                          = 0x0FC3  # U16 (internal reg 4035)
    IV_SCAN_DATA_POINTS                      = 100
    IV_TRACKER_BLOCK_SIZE                    = 200  # 100 points * 2 (V, I pairs per point)

    # IV scan data regions per PDF:
    # PV1: 5000-5199 (200 regs = 100 V/I points)
    # PV2: 5200-5399
    # PV3: 5400-5599
    PV1_IV_BASE                              = 0x1388  # 5000
    PV2_IV_BASE                              = 0x1450  # 5200
    PV3_IV_BASE                              = 0x1518  # 5400

    # =========================================================================
    # DER-AVM (Solarize convention, for simulator compat)
    # =========================================================================
    DEA_L1_CURRENT_LOW                       = 0x03E8  # S32
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

    # DER-AVM write registers
    DER_POWER_FACTOR_SET                     = 0x07D0
    DER_ACTION_MODE                          = 0x07D1
    DER_REACTIVE_POWER_PCT                   = 0x07D2
    DER_ACTIVE_POWER_PCT                     = 0x07D3
    INVERTER_ON_OFF                          = 0x0834

    # H01 aliases
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

    # Error codes (Kstar uses single DSP error code)
    ERROR_CODE1                              = DSP_ERROR_CODE
    ERROR_CODE2                              = DSP_ALARM_CODE
    ERROR_CODE3                              = DSP_ERROR_CODE_HIGH

    # Total PV power (sum of MPPT1-3 power — no dedicated register in Kstar,
    # use PV1_INPUT_POWER as primary, H01 can sum all MPPTs)
    PV_POWER                                 = PV1_INPUT_POWER
    PV_POWER_HIGH                            = PV1_INPUT_POWER_HIGH


    # --- Simulator/RTU compatible device info registers ---
    # Standardized at 0x1A00 (model, 16 regs = 32 bytes) and 0x1A10 (serial,
    # 8 regs = 16 bytes) so the equipment simulator's _populate_device_info
    # can write string data without colliding with measurement registers.
    # Matches the Solarize/Senergy/CPS convention for consistent behavior.
    DEVICE_MODEL                             = 0x1A00
    DEVICE_SERIAL_NUMBER                     = 0x1A10


class InverterMode:
    """Kstar operating mode (reg 3030 low byte). PDF Table 3.1.4."""
    INITIAL  = 0x00  # System initialization
    STANDBY  = 0x01  # Waiting
    STARTUP  = 0x02  # Pre-detection
    ON_GRID  = 0x03  # Normal
    FAULT    = 0x04  # Error
    SHUTDOWN = 0x05  # Permanent error
    AGING    = 0x06

    @classmethod
    def to_string(cls, v):
        return {
            0x00: 'INITIAL',
            0x01: 'STANDBY',
            0x02: 'STARTUP',
            0x03: 'ON_GRID',
            0x04: 'FAULT',
            0x05: 'PERMANENT_ERROR',
            0x06: 'AGING',
            0x07: 'INV_DSP_BURNING',
            0x08: 'ARM_BURNING',
            0x09: 'BST_DSP_BURNING',
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
    """DSP error code (Table 3.1.3)."""
    BITS = {
        0:  'GridVoltLow',
        1:  'GridVoltHigh',
        2:  'GridFrequencyLow',
        3:  'GridFrequencyHigh',
        4:  'BusVoltLow',
        5:  'BusVoltHigh',
        6:  'BusVoltUnbalance',
        7:  'IsolationFault',
        8:  'PVCurrentHigh',
        9:  'HardInverterCurrentOver',
        10: 'InverterCurrentOver',
        11: 'InverterDcCurrentOver',
        12: 'AmbientTemperatureOver',
        13: 'SinkTemperatureOver',
        14: 'ACRelayFault',
        16: 'RemoteOff',
        18: 'SPICommunicationFail',
        19: 'SPI2CommunicationFail',
        20: 'GFCIOverFault',
        21: 'GFCIDeviceFault',
        22: 'VoltageConsistentFault',
        23: 'FrequencyConsistentFault',
        25: 'AuxiliaryPowerOff',
        26: 'IGBTFault',
        27: 'NPEVoltFault',
        28: 'DCOverVoltSeriousFault',
        29: 'IGBTSeriousFault',
    }


class ErrorCode2:
    """DSP alarm code (Table 3.1.2)."""
    BITS = {
        0: 'FanALock',
        1: 'FanBLock',
        2: 'FanCLock',
        3: 'ZeroPower',
        4: 'ArrayWarning',
        6: 'LightningWarning',
        7: 'PVParallelOpen',
    }


class ErrorCode3:
    BITS = {}  # reserved


class KstarStatusConverter:
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
    'current':             0.01,
    'power':               1.0,   # Kstar uses W directly (not 0.1W)
    'frequency':           0.01,  # 0.01 Hz
    'power_factor':        0.001,
    'dea_current':         0.1,
    'dea_voltage':         0.1,
    'dea_active_power':    0.1,
    'dea_reactive_power':  1.0,
    'dea_frequency':       0.1,
    'iv_voltage':          0.1,
    'iv_current':          0.01,
}


# Channel configuration — 60K model: 3 MPPT, 9 strings (3 per MPPT)
MPPT_CHANNELS = 3
STRING_CHANNELS = 9


def registers_to_u32(low, high):
    return (high << 16) | low


def registers_to_s32(low, high):
    value = (high << 16) | low
    if value >= 0x80000000:
        value -= 0x100000000
    return value


def get_mppt_registers(mppt_num):
    """Return (voltage, current, power_low, power_high) for MPPT 1-3.

    Kstar PV voltages at 0x0BB8~0x0BBA, currents at 0x0BBB~0x0BBD,
    powers at 0x0BBE~0x0BC3 (S32, 2 regs each, stride 2).
    """
    if mppt_num < 1 or mppt_num > 3:
        raise ValueError(f"MPPT number must be 1-3, got {mppt_num}")
    v_addr = 0x0BB8 + (mppt_num - 1)      # 0x0BB8, 0x0BB9, 0x0BBA
    c_addr = 0x0BBB + (mppt_num - 1)      # 0x0BBB, 0x0BBC, 0x0BBD
    p_low  = 0x0BBE + (mppt_num - 1) * 2  # 0x0BBE, 0x0BC0, 0x0BC2
    p_high = p_low + 1
    return (v_addr, c_addr, p_low, p_high)


def get_string_registers(string_num):
    """Return (voltage_addr, current_addr) for string 1-9.

    9 strings = 3 MPPTs * 3 strings/MPPT.
    Each PV has 4 string slots; we use strings 1-3 of each.
    PV1 strings: 0x0BF8~0x0BFA (skip 0x0BFB = string 4)
    PV2 strings: 0x0BFC~0x0BFE (skip 0x0BFF)
    PV3 strings: 0x0C00~0x0C02 (skip 0x0C03)
    """
    if string_num < 1 or string_num > 9:
        raise ValueError(f"String number must be 1-9, got {string_num}")
    mppt_num = (string_num - 1) // 3 + 1
    str_in_mppt = (string_num - 1) % 3 + 1  # 1..3
    current_addr = 0x0BF8 + (mppt_num - 1) * 4 + (str_in_mppt - 1)
    v_addrs = {1: 0x0BB8, 2: 0x0BB9, 3: 0x0BBA}  # fallback to MPPT voltage
    return (v_addrs[mppt_num], current_addr)


def get_iv_tracker_voltage_registers(tracker_num, data_points=100):
    """Return IV voltage block for PV{tracker_num} (1-3).

    Kstar IV scan layout: PV1 at 5000-5199, PV2 at 5200-5399, PV3 at 5400-5599.
    Each data point = (voltage, current) pair, 100 points total = 200 regs.
    Voltage at even offsets, current at odd.
    """
    if tracker_num < 1 or tracker_num > 3:
        raise ValueError(f"Tracker number must be 1-3, got {tracker_num}")
    base = 0x1388 + (tracker_num - 1) * 200  # 5000, 5200, 5400
    return {'base': base, 'count': data_points * 2, 'end': base + data_points * 2 - 1}


def get_iv_string_current_registers(mppt_num, string_num, data_points=100):
    """Kstar has only PV-level IV scan, not per-string. Returns the same base
    as get_iv_tracker_voltage_registers but with current offset."""
    if mppt_num < 1 or mppt_num > 3:
        raise ValueError(f"MPPT number must be 1-3, got {mppt_num}")
    if string_num < 1 or string_num > 3:
        raise ValueError(f"String number must be 1-3 per MPPT, got {string_num}")
    base = 0x1388 + (mppt_num - 1) * 200 + 1  # current at offset 1 (V, I, V, I, ...)
    return {'base': base, 'count': data_points * 2, 'end': base + data_points * 2 - 1}


def get_iv_string_mapping(total_strings=9, strings_per_mppt=3):
    """Return mapping of string index → IV scan register base addresses.

    Kstar IV scan is PV-level (not per string). All strings of same PV
    share the same voltage/current arrays.
    """
    mapping = []
    data_points = 100
    for string_idx in range(total_strings):
        mppt_num       = (string_idx // strings_per_mppt) + 1
        string_in_mppt = (string_idx % strings_per_mppt) + 1
        v_regs = get_iv_tracker_voltage_registers(mppt_num, data_points)
        i_regs = get_iv_string_current_registers(mppt_num, string_in_mppt, data_points)
        mapping.append({
            'string_num':     string_idx + 1,
            'total_strings':  total_strings,
            'mppt_num':       mppt_num,
            'string_in_mppt': string_in_mppt,
            'voltage_base':   v_regs['base'],
            'current_base':   i_regs['base'],
            'data_points':    data_points,
        })
    return mapping


def generate_iv_voltage_data(voc, v_min, data_points=100):
    """Generate IV scan voltage array (U16 0.1V, ascending v_min->voc)."""
    step = (voc - v_min) / max(data_points - 1, 1)
    return [int((v_min + step * i) * 10) & 0xFFFF for i in range(data_points)]


def generate_iv_current_data(isc, voc, v_min, data_points=100):
    """Generate IV scan current array using IV curve approximation."""
    step = (voc - v_min) / max(data_points - 1, 1)
    regs = []
    for i in range(data_points):
        v = v_min + step * i
        ratio = v / voc if voc > 0 else 0
        current = max(0.0, isc * (1.0 - ratio ** 20))
        regs.append(int(current * 100) & 0xFFFF)
    return regs


DATA_TYPES = {
    'PV1_INPUT_VOLTAGE': 'U16',
    'PV2_INPUT_VOLTAGE': 'U16',
    'PV3_INPUT_VOLTAGE': 'U16',
    'PV1_INPUT_CURRENT': 'U16',
    'PV2_INPUT_CURRENT': 'U16',
    'PV3_INPUT_CURRENT': 'U16',
    'PV1_INPUT_POWER': 'S32',
    'PV2_INPUT_POWER': 'S32',
    'PV3_INPUT_POWER': 'S32',
    'MPPT1_VOLTAGE': 'U16',
    'MPPT1_CURRENT': 'U16',
    'MPPT1_POWER': 'S32',
    'MPPT2_VOLTAGE': 'U16',
    'MPPT2_CURRENT': 'U16',
    'MPPT2_POWER': 'S32',
    'MPPT3_VOLTAGE': 'U16',
    'MPPT3_CURRENT': 'U16',
    'MPPT3_POWER': 'S32',
    'L1_VOLTAGE': 'U16',
    'L2_VOLTAGE': 'U16',
    'L3_VOLTAGE': 'U16',
    'L1_CURRENT': 'U16',
    'L2_CURRENT': 'U16',
    'L3_CURRENT': 'U16',
    'FREQUENCY': 'U16',
    'AC_POWER': 'S32',
    'INNER_TEMP': 'S16',
    'MODULE_TEMPERATURE': 'S16',
    'INVERTER_MODE': 'U16',
    'DSP_ALARM_CODE': 'U16',
    'DSP_ERROR_CODE': 'U32',
    'CUMULATIVE_ENERGY': 'U32',
    'POWER_FACTOR': 'U16',
    'ERROR_CODE1': 'U32',
    'ERROR_CODE2': 'U16',
    'ERROR_CODE3': 'U16',
    # String currents — Kstar uses S16
    'PV1_STRING_CURRENT_1': 'S16',
    'PV1_STRING_CURRENT_2': 'S16',
    'PV1_STRING_CURRENT_3': 'S16',
    'PV2_STRING_CURRENT_1': 'S16',
    'PV2_STRING_CURRENT_2': 'S16',
    'PV2_STRING_CURRENT_3': 'S16',
    'PV3_STRING_CURRENT_1': 'S16',
    'PV3_STRING_CURRENT_2': 'S16',
    'PV3_STRING_CURRENT_3': 'S16',
    'STRING1_CURRENT': 'S16',
    'STRING2_CURRENT': 'S16',
    'STRING3_CURRENT': 'S16',
    'STRING4_CURRENT': 'S16',
    'STRING5_CURRENT': 'S16',
    'STRING6_CURRENT': 'S16',
    'STRING7_CURRENT': 'S16',
    'STRING8_CURRENT': 'S16',
    'STRING9_CURRENT': 'S16',
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


# RTU batch read blocks — Kstar uses FC04 (Read Input Registers)
# Block 1: DER-AVM (0x03E8-0x03FD, 22 regs)
# Block 2: PV inputs + grid data (0x0BB8-0x0BD6, 31 regs) —
#          covers MPPT1-3 V/I/P, grid V/I/F, power, temps, mode
# Block 3: Cumulative energy (0x0BDE-0x0BDF, 2 regs)
# Block 4: Power factor (0x0BF0, 1 reg)
# Block 5: String currents 9 (0x0BF8-0x0C02, 11 regs)
# Block 6: IV scan status (0x0C36, 1 reg)
READ_BLOCKS = [
    {'start': 0x03E8, 'count':  22, 'fc': 3},   # DER-AVM (written by sim on FC3)
    {'start': 0x0BB8, 'count':  31, 'fc': 4},   # PV/grid block
    {'start': 0x0BDE, 'count':   2, 'fc': 4},
    {'start': 0x0BF0, 'count':   1, 'fc': 4},
    {'start': 0x0BF8, 'count':  11, 'fc': 4},
    {'start': 0x0C36, 'count':   1, 'fc': 4},
]


# H01 field → RegisterMap attribute
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


U32_WORD_ORDER = 'LH'
RTU_FC_CODE = 4  # Kstar uses FC04 (Read Input Registers)
