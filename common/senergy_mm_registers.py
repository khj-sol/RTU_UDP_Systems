# -*- coding: utf-8 -*-
"""
Senergy Inverter Modbus Register Map
Based on Senergy Modbus Protocol V1.2.4 (Korea)
Protocol Name: senergy
MPPT: 9 | Strings: 24 (4 strings/MPPT) | IV Scan: Yes | DER-AVM: Yes | DEA Monitor: Yes

Same protocol base as Solarize. Registers 0x1001-0x104C, 0x03E8-0x03FD, 0x07D0-0x0835,
0x6001-0x600D are identical. Extended: MPPTs 5-9 at 0x1080+, Strings 9-24 at 0x1060+,
IV scan: 9 trackers x 4 strings x 64 data points.
"""


class RegisterMap:
    """Senergy Modbus Register Map - Solarize Protocol Compatible"""

    # =========================================================================
    # Device Information (0x1A00-0x1A90)
    # =========================================================================
    DEVICE_MODEL            = 0x1A00  # 8 regs, ASCII string
    SERIAL_NUMBER           = 0x1A10  # 8 regs, ASCII string
    MASTER_FIRMWARE_VERSION = 0x1A1C  # 3 regs, ASCII string
    SLAVE_FIRMWARE_VERSION  = 0x1A26  # 3 regs, ASCII string
    MPPT_COUNT              = 0x1A3B  # 1 reg, U16
    NOMINAL_VOLTAGE         = 0x1A44  # 1 reg, U16, scale 0.1V
    NOMINAL_FREQUENCY       = 0x1A45  # 1 reg, U16, scale 0.01Hz
    NOMINAL_POWER_LOW       = 0x1A46  # 1 reg, U16, 1W
    NOMINAL_POWER_HIGH      = 0x1A4E  # 1 reg, U16, 1W (high word)
    GRID_PHASE_NUMBER       = 0x1A48  # 1 reg, U16: 1=single, 2=split, 3=three
    EMS_FIRMWARE_VERSION    = 0x1A60  # 3 regs, ASCII string
    LCD_FIRMWARE_VERSION    = 0x1A8E  # 3 regs, ASCII string

    # Alias
    FIRMWARE_VERSION = MASTER_FIRMWARE_VERSION

    # =========================================================================
    # AC Side Real-time Data (0x1001-0x100F)
    # =========================================================================
    L1_VOLTAGE     = 0x1001  # U16, scale 0.1V
    L1_CURRENT     = 0x1002  # U16, scale 0.01A
    L1_POWER_LOW   = 0x1003  # S32, scale 0.1W
    L1_POWER_HIGH  = 0x1004
    L1_FREQUENCY   = 0x1005  # U16, scale 0.01Hz
    L2_VOLTAGE     = 0x1006  # U16, scale 0.1V
    L2_CURRENT     = 0x1007  # U16, scale 0.01A
    L2_POWER_LOW   = 0x1008  # S32, scale 0.1W
    L2_POWER_HIGH  = 0x1009
    L2_FREQUENCY   = 0x100A  # U16, scale 0.01Hz
    L3_VOLTAGE     = 0x100B  # U16, scale 0.1V
    L3_CURRENT     = 0x100C  # U16, scale 0.01A
    L3_POWER_LOW   = 0x100D  # S32, scale 0.1W
    L3_POWER_HIGH  = 0x100E
    L3_FREQUENCY   = 0x100F  # U16, scale 0.01Hz

    # R/S/T Phase Aliases
    R_VOLTAGE    = L1_VOLTAGE
    R_CURRENT    = L1_CURRENT
    R_POWER_LOW  = L1_POWER_LOW
    R_POWER_HIGH = L1_POWER_HIGH
    R_FREQUENCY  = L1_FREQUENCY
    S_VOLTAGE    = L2_VOLTAGE
    S_CURRENT    = L2_CURRENT
    S_POWER_LOW  = L2_POWER_LOW
    S_POWER_HIGH = L2_POWER_HIGH
    S_FREQUENCY  = L2_FREQUENCY
    T_VOLTAGE    = L3_VOLTAGE
    T_CURRENT    = L3_CURRENT
    T_POWER_LOW  = L3_POWER_LOW
    T_POWER_HIGH = L3_POWER_HIGH
    T_FREQUENCY  = L3_FREQUENCY

    # =========================================================================
    # MPPT Data (0x1010-0x1093)
    # MPPTs 1-3: stride-4 starting at 0x1010
    # MPPT  4:   0x103E-0x1041
    # MPPTs 5-9: stride-4 starting at 0x1080
    # =========================================================================
    MPPT1_VOLTAGE    = 0x1010  # U16, scale 0.1V
    MPPT1_CURRENT    = 0x1011  # S16, scale 0.01A
    MPPT1_POWER_LOW  = 0x1012  # U32, scale 0.1W
    MPPT1_POWER_HIGH = 0x1013
    MPPT2_VOLTAGE    = 0x1014
    MPPT2_CURRENT    = 0x1015
    MPPT2_POWER_LOW  = 0x1016
    MPPT2_POWER_HIGH = 0x1017
    MPPT3_VOLTAGE    = 0x1018
    MPPT3_CURRENT    = 0x1019
    MPPT3_POWER_LOW  = 0x101A
    MPPT3_POWER_HIGH = 0x101B
    # MPPT 4 (separate block)
    MPPT4_VOLTAGE    = 0x103E
    MPPT4_CURRENT    = 0x103F
    MPPT4_POWER_LOW  = 0x1040
    MPPT4_POWER_HIGH = 0x1041
    # MPPTs 5-9 (stride-4 starting at 0x1080)
    MPPT5_VOLTAGE    = 0x1080
    MPPT5_CURRENT    = 0x1081
    MPPT5_POWER_LOW  = 0x1082
    MPPT5_POWER_HIGH = 0x1083
    MPPT6_VOLTAGE    = 0x1084
    MPPT6_CURRENT    = 0x1085
    MPPT6_POWER_LOW  = 0x1086
    MPPT6_POWER_HIGH = 0x1087
    MPPT7_VOLTAGE    = 0x1088
    MPPT7_CURRENT    = 0x1089
    MPPT7_POWER_LOW  = 0x108A
    MPPT7_POWER_HIGH = 0x108B
    MPPT8_VOLTAGE    = 0x108C
    MPPT8_CURRENT    = 0x108D
    MPPT8_POWER_LOW  = 0x108E
    MPPT8_POWER_HIGH = 0x108F
    MPPT9_VOLTAGE    = 0x1090
    MPPT9_CURRENT    = 0x1091
    MPPT9_POWER_LOW  = 0x1092
    MPPT9_POWER_HIGH = 0x1093

    # =========================================================================
    # Status & Temperature (0x101C-0x1020)
    # =========================================================================
    INNER_TEMP    = 0x101C  # S16, 1°C
    INVERTER_MODE = 0x101D  # U16, see InverterMode class
    ERROR_CODE1   = 0x101E  # U16, bit field -> alarm1
    ERROR_CODE2   = 0x101F  # U16, bit field -> alarm2
    ERROR_CODE3   = 0x1020  # U16, bit field -> alarm3

    # =========================================================================
    # Energy Data (0x1021-0x1028)
    # =========================================================================
    TOTAL_ENERGY_LOW           = 0x1021  # U32, kWh
    TOTAL_ENERGY_HIGH          = 0x1022
    TOTAL_GENERATION_TIME_LOW  = 0x1023  # U32, Hour
    TOTAL_GENERATION_TIME_HIGH = 0x1024
    TODAY_ENERGY_LOW           = 0x1027  # U32, Wh
    TODAY_ENERGY_HIGH          = 0x1028

    # =========================================================================
    # Grid Power Data (0x1034-0x103D)
    # =========================================================================
    FUSE_OPEN_DATA_LOW             = 0x1034  # U16, bit field
    FUSE_OPEN_DATA_HIGH            = 0x1035  # U16, bit field
    GRID_TOTAL_ACTIVE_POWER_LOW    = 0x1037  # S32, scale 0.1W (L1+L2+L3)
    GRID_TOTAL_ACTIVE_POWER_HIGH   = 0x1038
    GRID_TOTAL_REACTIVE_POWER_LOW  = 0x1039  # S32, scale 0.1Var
    GRID_TOTAL_REACTIVE_POWER_HIGH = 0x103A
    PV_TODAY_PEAK_POWER_LOW        = 0x103B  # S32, scale 0.1W
    PV_TODAY_PEAK_POWER_HIGH       = 0x103C
    POWER_FACTOR                   = 0x103D  # S16, scale 0.001

    # Aliases
    GRID_POWER_LOW  = GRID_TOTAL_ACTIVE_POWER_LOW
    GRID_POWER_HIGH = GRID_TOTAL_ACTIVE_POWER_HIGH

    # =========================================================================
    # PV Total Power (0x1048-0x104C)
    # =========================================================================
    PV_TOTAL_INPUT_POWER_LOW  = 0x1048  # U32, scale 0.1W (all MPPT sum)
    PV_TOTAL_INPUT_POWER_HIGH = 0x1049
    TOTAL_ENERGY_DECIMALS     = 0x104C  # U16, Wh (decimal part of TOTAL_ENERGY)

    # Aliases
    PV_POWER_LOW  = PV_TOTAL_INPUT_POWER_LOW
    PV_POWER_HIGH = PV_TOTAL_INPUT_POWER_HIGH

    # --- Standard handler compatibility aliases (H01 Body Type 4 required) ---
    R_PHASE_VOLTAGE = L1_VOLTAGE
    S_PHASE_VOLTAGE = L2_VOLTAGE
    T_PHASE_VOLTAGE = L3_VOLTAGE
    R_PHASE_CURRENT = L1_CURRENT
    S_PHASE_CURRENT = L2_CURRENT
    T_PHASE_CURRENT = L3_CURRENT
    FREQUENCY       = L1_FREQUENCY
    AC_POWER        = GRID_TOTAL_ACTIVE_POWER_LOW
    PV_POWER        = PV_TOTAL_INPUT_POWER_LOW
    TOTAL_ENERGY    = TOTAL_ENERGY_LOW

    # =========================================================================
    # String Input Data (0x1050-0x107F) — 24 strings, voltage/current pairs
    # Strings 1-8:  0x1050-0x105F  (same as Solarize)
    # Strings 9-24: 0x1060-0x107F  (Senergy extension)
    # =========================================================================
    STRING1_VOLTAGE  = 0x1050  # U16, scale 0.1V
    STRING1_CURRENT  = 0x1051  # S16, scale 0.01A
    STRING2_VOLTAGE  = 0x1052
    STRING2_CURRENT  = 0x1053
    STRING3_VOLTAGE  = 0x1054
    STRING3_CURRENT  = 0x1055
    STRING4_VOLTAGE  = 0x1056
    STRING4_CURRENT  = 0x1057
    STRING5_VOLTAGE  = 0x1058
    STRING5_CURRENT  = 0x1059
    STRING6_VOLTAGE  = 0x105A
    STRING6_CURRENT  = 0x105B
    STRING7_VOLTAGE  = 0x105C
    STRING7_CURRENT  = 0x105D
    STRING8_VOLTAGE  = 0x105E
    STRING8_CURRENT  = 0x105F
    STRING9_VOLTAGE  = 0x1060
    STRING9_CURRENT  = 0x1061
    STRING10_VOLTAGE = 0x1062
    STRING10_CURRENT = 0x1063
    STRING11_VOLTAGE = 0x1064
    STRING11_CURRENT = 0x1065
    STRING12_VOLTAGE = 0x1066
    STRING12_CURRENT = 0x1067
    STRING13_VOLTAGE = 0x1068
    STRING13_CURRENT = 0x1069
    STRING14_VOLTAGE = 0x106A
    STRING14_CURRENT = 0x106B
    STRING15_VOLTAGE = 0x106C
    STRING15_CURRENT = 0x106D
    STRING16_VOLTAGE = 0x106E
    STRING16_CURRENT = 0x106F
    STRING17_VOLTAGE = 0x1070
    STRING17_CURRENT = 0x1071
    STRING18_VOLTAGE = 0x1072
    STRING18_CURRENT = 0x1073
    STRING19_VOLTAGE = 0x1074
    STRING19_CURRENT = 0x1075
    STRING20_VOLTAGE = 0x1076
    STRING20_CURRENT = 0x1077
    STRING21_VOLTAGE = 0x1078
    STRING21_CURRENT = 0x1079
    STRING22_VOLTAGE = 0x107A
    STRING22_CURRENT = 0x107B
    STRING23_VOLTAGE = 0x107C
    STRING23_CURRENT = 0x107D
    STRING24_VOLTAGE = 0x107E
    STRING24_CURRENT = 0x107F

    # =========================================================================
    # DEA-AVM Real-time Data (0x03E8-0x03FD) — H05 Body Type 14
    # =========================================================================
    DEA_L1_CURRENT_LOW           = 0x03E8  # S32, scale 0.1A
    DEA_L1_CURRENT_HIGH          = 0x03E9
    DEA_L2_CURRENT_LOW           = 0x03EA  # S32, scale 0.1A
    DEA_L2_CURRENT_HIGH          = 0x03EB
    DEA_L3_CURRENT_LOW           = 0x03EC  # S32, scale 0.1A
    DEA_L3_CURRENT_HIGH          = 0x03ED
    DEA_L1_VOLTAGE_LOW           = 0x03EE  # S32, scale 0.1V
    DEA_L1_VOLTAGE_HIGH          = 0x03EF
    DEA_L2_VOLTAGE_LOW           = 0x03F0  # S32, scale 0.1V
    DEA_L2_VOLTAGE_HIGH          = 0x03F1
    DEA_L3_VOLTAGE_LOW           = 0x03F2  # S32, scale 0.1V
    DEA_L3_VOLTAGE_HIGH          = 0x03F3
    DEA_TOTAL_ACTIVE_POWER_LOW   = 0x03F4  # S32, scale 0.1kW
    DEA_TOTAL_ACTIVE_POWER_HIGH  = 0x03F5
    DEA_TOTAL_REACTIVE_POWER_LOW  = 0x03F6  # S32, scale 1 Var
    DEA_TOTAL_REACTIVE_POWER_HIGH = 0x03F7
    DEA_POWER_FACTOR_LOW         = 0x03F8  # S32, scale 0.001
    DEA_POWER_FACTOR_HIGH        = 0x03F9
    DEA_FREQUENCY_LOW            = 0x03FA  # S32, scale 0.1Hz
    DEA_FREQUENCY_HIGH           = 0x03FB
    DEA_STATUS_FLAG_LOW          = 0x03FC  # S32, bit field
    DEA_STATUS_FLAG_HIGH         = 0x03FD

    # =========================================================================
    # DER-AVM Control Parameters (0x07D0-0x0835) — H05 Body Type 13
    # =========================================================================
    DER_POWER_FACTOR_SET       = 0x07D0  # S16, scale 0.001, [-1000,-800],[800,1000]
    DER_ACTION_MODE            = 0x07D1  # U16: 0=self, 2=DER-AVM, 5=Q(V)
    DER_REACTIVE_POWER_PCT     = 0x07D2  # S16, scale 0.1%, [-484, 484]
    DER_ACTIVE_POWER_PCT       = 0x07D3  # U16, scale 0.1%, [0, 1100]
    INVERTER_ON_OFF            = 0x0834  # U16: 0=ON, 1=OFF
    CLEAR_PV_INSULATION_WARNING = 0x0835  # U16: 0=Non-active, 1=Clear

    # Aliases for backward compatibility
    POWER_FACTOR_SET   = DER_POWER_FACTOR_SET
    ACTION_MODE        = DER_ACTION_MODE
    REACTIVE_POWER_PCT = DER_REACTIVE_POWER_PCT
    ACTIVE_POWER_PCT   = DER_ACTIVE_POWER_PCT
    OPERATION_MODE     = DER_ACTION_MODE
    REACTIVE_POWER_SET = DER_REACTIVE_POWER_PCT

    # =========================================================================
    # Inverter Control (0x6001-0x6010)
    # =========================================================================
    INVERTER_CONTROL       = 0x6001  # U16: 0=power on, 1=shut down
    EEPROM_DEFAULT         = 0x6006  # U16: 0=not default, 1=default
    CLEAR_ENERGY_RECORD    = 0x6009  # U16: 0=not clear, 1=clear
    IV_CURVE_SCAN          = 0x600D  # U16: W 0=Stop,1=Start; R 0=Idle,1=Running,2=Finished
    POWER_FACTOR_DYNAMIC   = 0x600F  # S16, scale 0.001 (dynamic control)
    REACTIVE_POWER_DYNAMIC = 0x6010  # S16, scale 0.01% (dynamic control)

    # Alias
    IV_SCAN_COMMAND = IV_CURVE_SCAN
    IV_SCAN_STATUS  = IV_CURVE_SCAN

    POWER_DERATING_PCT = 0x3005  # U16, [0-110]%, dynamic active power

    # =========================================================================
    # IV Scan Data Registers (0x8000-0x8B3F)
    # 9 trackers × 5 blocks × 64 registers = block_size=0x140 per tracker
    # Layout per tracker: [voltage_64][string1_64][string2_64][string3_64][string4_64]
    # =========================================================================
    # Tracker 1  (0x8000-0x813F)
    IV_TRACKER1_VOLTAGE_BASE  = 0x8000
    IV_STRING1_1_CURRENT_BASE = 0x8040
    IV_STRING1_2_CURRENT_BASE = 0x8080
    IV_STRING1_3_CURRENT_BASE = 0x80C0
    IV_STRING1_4_CURRENT_BASE = 0x8100
    # Tracker 2  (0x8140-0x827F)
    IV_TRACKER2_VOLTAGE_BASE  = 0x8140
    IV_STRING2_1_CURRENT_BASE = 0x8180
    IV_STRING2_2_CURRENT_BASE = 0x81C0
    IV_STRING2_3_CURRENT_BASE = 0x8200
    IV_STRING2_4_CURRENT_BASE = 0x8240
    # Tracker 3  (0x8280-0x83BF)
    IV_TRACKER3_VOLTAGE_BASE  = 0x8280
    IV_STRING3_1_CURRENT_BASE = 0x82C0
    IV_STRING3_2_CURRENT_BASE = 0x8300
    IV_STRING3_3_CURRENT_BASE = 0x8340
    IV_STRING3_4_CURRENT_BASE = 0x8380
    # Tracker 4  (0x83C0-0x84FF)
    IV_TRACKER4_VOLTAGE_BASE  = 0x83C0
    IV_STRING4_1_CURRENT_BASE = 0x8400
    IV_STRING4_2_CURRENT_BASE = 0x8440
    IV_STRING4_3_CURRENT_BASE = 0x8480
    IV_STRING4_4_CURRENT_BASE = 0x84C0
    # Tracker 5  (0x8500-0x863F)
    IV_TRACKER5_VOLTAGE_BASE  = 0x8500
    IV_STRING5_1_CURRENT_BASE = 0x8540
    IV_STRING5_2_CURRENT_BASE = 0x8580
    IV_STRING5_3_CURRENT_BASE = 0x85C0
    IV_STRING5_4_CURRENT_BASE = 0x8600
    # Tracker 6  (0x8640-0x877F)
    IV_TRACKER6_VOLTAGE_BASE  = 0x8640
    IV_STRING6_1_CURRENT_BASE = 0x8680
    IV_STRING6_2_CURRENT_BASE = 0x86C0
    IV_STRING6_3_CURRENT_BASE = 0x8700
    IV_STRING6_4_CURRENT_BASE = 0x8740
    # Tracker 7  (0x8780-0x88BF)
    IV_TRACKER7_VOLTAGE_BASE  = 0x8780
    IV_STRING7_1_CURRENT_BASE = 0x87C0
    IV_STRING7_2_CURRENT_BASE = 0x8800
    IV_STRING7_3_CURRENT_BASE = 0x8840
    IV_STRING7_4_CURRENT_BASE = 0x8880
    # Tracker 8  (0x88C0-0x89FF)
    IV_TRACKER8_VOLTAGE_BASE  = 0x88C0
    IV_STRING8_1_CURRENT_BASE = 0x8900
    IV_STRING8_2_CURRENT_BASE = 0x8940
    IV_STRING8_3_CURRENT_BASE = 0x8980
    IV_STRING8_4_CURRENT_BASE = 0x89C0
    # Tracker 9  (0x8A00-0x8B3F)
    IV_TRACKER9_VOLTAGE_BASE  = 0x8A00
    IV_STRING9_1_CURRENT_BASE = 0x8A40
    IV_STRING9_2_CURRENT_BASE = 0x8A80
    IV_STRING9_3_CURRENT_BASE = 0x8AC0
    IV_STRING9_4_CURRENT_BASE = 0x8B00

    IV_SCAN_DATA_POINTS   = 64
    IV_TRACKER_BLOCK_SIZE = 0x140  # 5 × 64 registers per tracker


class IVScanCommand:
    """IV Scan Command values for writing to 0x600D"""
    NON_ACTIVE = 0x0000
    ACTIVE     = 0x0001


class IVScanStatus:
    """IV Scan Status values when reading from 0x600D"""
    IDLE     = 0x0000
    RUNNING  = 0x0001
    FINISHED = 0x0002

    @classmethod
    def to_string(cls, status):
        return {0: "Idle", 1: "Running", 2: "Finished"}.get(status, f"Unknown({status})")


class InverterMode:
    """Inverter Mode Table (0x101D) — identical to Solarize"""
    INITIAL  = 0x00
    STANDBY  = 0x01
    ON_GRID  = 0x03
    OFF_GRID = 0x04
    FAULT    = 0x05
    SHUTDOWN = 0x09

    @classmethod
    def to_string(cls, mode):
        return {
            0x00: "Initial",
            0x01: "Standby",
            0x03: "On-Grid",
            0x04: "Off-Grid",
            0x05: "Fault",
            0x09: "Shutdown",
        }.get(mode, f"Unknown({mode})")


class DerActionMode:
    """DER-AVM Action Mode (0x07D1)"""
    SELF_CONTROL    = 0
    DER_AVM_CONTROL = 2
    QV_CONTROL      = 5


class ErrorCode1:
    """Error Code Table1 (0x101E) — Bit field -> alarm1"""
    BITS = {
        0:  "Inverter over dc-bias current",
        1:  "Inverter relay abnormal",
        2:  "Remote off",
        3:  "Inverter over temperature",
        4:  "GFCI abnormal",
        5:  "PV string reverse",
        6:  "System type error",
        7:  "Fan abnormal",
        8:  "Dc-link unbalance or under voltage",
        9:  "Dc-link over voltage",
        10: "Internal communication error",
        11: "Software incompatibility",
        12: "Internal storage error",
        13: "Data inconsistency",
        14: "Inverter abnormal",
        15: "Boost abnormal",
    }

    @classmethod
    def decode(cls, value):
        return [f"E{b}:{d}" for b, d in cls.BITS.items() if value & (1 << b)]

    @classmethod
    def to_string(cls, value):
        return ", ".join(cls.decode(value)) if value else "OK"


class ErrorCode2:
    """Error Code Table2 (0x101F) — Bit field -> alarm2"""
    BITS = {
        0:  "Grid over voltage",
        1:  "Grid under voltage",
        2:  "Grid absent",
        3:  "Grid over frequency",
        4:  "Grid under frequency",
        5:  "PV over voltage",
        6:  "PV insulation abnormal",
        7:  "Leakage current abnormal",
        8:  "Inverter in power limit state",
        9:  "Internal power supply abnormal",
        10: "PV string abnormal",
        11: "PV under voltage",
        12: "PV irradiation weak",
        13: "Grid abnormal",
        14: "Arc fault detection",
        15: "AC moving average voltage high",
    }

    @classmethod
    def decode(cls, value):
        return [f"E{b}:{d}" for b, d in cls.BITS.items() if value & (1 << b)]

    @classmethod
    def to_string(cls, value):
        return ", ".join(cls.decode(value)) if value else "OK"


class ErrorCode3:
    """Error Code Table3 (0x1020) — Bit field -> alarm3"""
    BITS = {
        0:  "Reserved",
        1:  "Logger/E-Display EEPROM fail",
        2:  "Reserved",
        3:  "Single tracker detect warning",
        4:  "AFCI lost",
        5:  "Data logger lost",
        6:  "Meter lost",
        7:  "Inverter lost",
        8:  "Grid N abnormal",
        9:  "Surge Protection Devices (SPD) defective",
        10: "Parallel ID warning",
        11: "Parallel SYN signal warning",
        12: "Parallel BAT abnormal",
        13: "Parallel GRID abnormal",
        14: "Generator voltage abnormal",
        15: "Reserved",
    }

    @classmethod
    def decode(cls, value):
        return [f"E{b}:{d}" for b, d in cls.BITS.items() if value & (1 << b)]

    @classmethod
    def to_string(cls, value):
        return ", ".join(cls.decode(value)) if value else "OK"


# Scale factors — identical to Solarize protocol
SCALE = {
    'voltage':            0.1,
    'current':            0.01,
    'power':              0.1,
    'frequency':          0.01,
    'power_factor':       0.001,
    'dea_current':        0.1,
    'dea_voltage':        0.1,
    'dea_active_power':   0.1,
    'dea_reactive_power': 1,
    'dea_frequency':      0.1,
    'iv_voltage':         0.1,
    'iv_current':         0.1,
}


def registers_to_u32(low, high):
    """Combine two U16 to U32"""
    return (high << 16) | low


def registers_to_s32(low, high):
    """Combine two U16 to S32"""
    value = (high << 16) | low
    if value >= 0x80000000:
        value -= 0x100000000
    return value


def get_string_registers(string_num):
    """Return (voltage_addr, current_addr) for a string number (1-24)."""
    if string_num < 1 or string_num > 24:
        raise ValueError(f"String number must be 1-24, got {string_num}")
    base = 0x1050 + (string_num - 1) * 2
    return (base, base + 1)


def get_mppt_registers(mppt_num):
    """Return (voltage, current, power_low, power_high) for MPPT number (1-9)."""
    if mppt_num < 1 or mppt_num > 9:
        raise ValueError(f"MPPT number must be 1-9, got {mppt_num}")
    if mppt_num <= 3:
        base = 0x1010 + (mppt_num - 1) * 4
    elif mppt_num == 4:
        base = 0x103E
    else:  # 5-9
        base = 0x1080 + (mppt_num - 5) * 4
    return (base, base + 1, base + 2, base + 3)


def get_iv_tracker_voltage_registers(tracker_num, data_points=64):
    """Return {'base', 'count', 'end'} for IV voltage block of a tracker (1-9)."""
    if tracker_num < 1 or tracker_num > 9:
        raise ValueError(f"Tracker number must be 1-9, got {tracker_num}")
    base = 0x8000 + (tracker_num - 1) * RegisterMap.IV_TRACKER_BLOCK_SIZE
    return {'base': base, 'count': data_points, 'end': base + data_points - 1}


def get_iv_string_current_registers(mppt_num, string_num, data_points=64):
    """Return {'base', 'count', 'end'} for IV current block of a string (string_num 1-4)."""
    if mppt_num < 1 or mppt_num > 9:
        raise ValueError(f"MPPT number must be 1-9, got {mppt_num}")
    if string_num < 1 or string_num > 4:
        raise ValueError(f"String number must be 1-4 per MPPT, got {string_num}")
    tracker_base = 0x8000 + (mppt_num - 1) * RegisterMap.IV_TRACKER_BLOCK_SIZE
    base = tracker_base + string_num * 0x40  # voltage at offset 0, strings at 0x40/0x80/0xC0/0x100
    return {'base': base, 'count': data_points, 'end': base + data_points - 1}


def get_iv_string_mapping(total_strings=24, strings_per_mppt=4):
    """Return list of dicts mapping string index to IV scan register addresses."""
    mapping = []
    data_points = RegisterMap.IV_SCAN_DATA_POINTS
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


def generate_iv_voltage_data(voc, v_min, data_points=64):
    """Generate IV scan voltage array (U16, 0.1V units, ascending v_min→voc)."""
    step = (voc - v_min) / max(data_points - 1, 1)
    return [int((v_min + step * i) * 10) & 0xFFFF for i in range(data_points)]


def generate_iv_current_data(isc, voc, v_min, data_points=64):
    """Generate IV scan current array (U16, 0.01A units) using IV curve approximation."""
    step = (voc - v_min) / max(data_points - 1, 1)
    regs = []
    for i in range(data_points):
        v = v_min + step * i
        ratio = v / voc if voc > 0 else 0
        current = max(0.0, isc * (1.0 - ratio ** 20))
        regs.append(int(current * 100) & 0xFFFF)
    return regs


class SenergyStatusConverter:
    """Senergy INVERTER_MODE (0x101D) is directly InverterMode-compatible."""

    @classmethod
    def to_inverter_mode(cls, raw):
        return raw


# Dynamic-loader alias required by modbus_handler.load_register_module
StatusConverter = SenergyStatusConverter


DATA_TYPES = {
    # AC monitoring
    'L1_VOLTAGE': 'U16', 'L1_CURRENT': 'U16',
    'L2_VOLTAGE': 'U16', 'L2_CURRENT': 'U16',
    'L3_VOLTAGE': 'U16', 'L3_CURRENT': 'U16',
    'L1_FREQUENCY': 'U16', 'L2_FREQUENCY': 'U16', 'L3_FREQUENCY': 'U16',
    'GRID_TOTAL_ACTIVE_POWER_LOW': 'S32', 'POWER_FACTOR': 'S16',
    'TOTAL_ENERGY_LOW': 'U32',
    'TODAY_ENERGY_LOW': 'U32',
    'INVERTER_MODE': 'U16',
    'ERROR_CODE1': 'U16', 'ERROR_CODE2': 'U16', 'ERROR_CODE3': 'U16',
    'INNER_TEMP': 'S16',
    'PV_TOTAL_INPUT_POWER_LOW': 'U32',
    # MPPTs 1-9
    'MPPT1_VOLTAGE': 'U16', 'MPPT1_CURRENT': 'S16',
    'MPPT2_VOLTAGE': 'U16', 'MPPT2_CURRENT': 'S16',
    'MPPT3_VOLTAGE': 'U16', 'MPPT3_CURRENT': 'S16',
    'MPPT4_VOLTAGE': 'U16', 'MPPT4_CURRENT': 'S16',
    'MPPT5_VOLTAGE': 'U16', 'MPPT5_CURRENT': 'S16',
    'MPPT6_VOLTAGE': 'U16', 'MPPT6_CURRENT': 'S16',
    'MPPT7_VOLTAGE': 'U16', 'MPPT7_CURRENT': 'S16',
    'MPPT8_VOLTAGE': 'U16', 'MPPT8_CURRENT': 'S16',
    'MPPT9_VOLTAGE': 'U16', 'MPPT9_CURRENT': 'S16',
    # Strings 1-24
    **{f'STRING{n}_VOLTAGE': 'U16' for n in range(1, 25)},
    **{f'STRING{n}_CURRENT': 'S16' for n in range(1, 25)},
    # DER control
    'DER_POWER_FACTOR_SET': 'S16', 'DER_ACTION_MODE': 'U16',
    'DER_REACTIVE_POWER_PCT': 'S16', 'DER_ACTIVE_POWER_PCT': 'U16',
    'INVERTER_ON_OFF': 'U16', 'IV_CURVE_SCAN': 'U16',
    'POWER_FACTOR_DYNAMIC': 'S16', 'REACTIVE_POWER_DYNAMIC': 'S16',
    'POWER_DERATING_PCT': 'U16',
    # DEA monitor (all S32 pairs)
    'DEA_L1_CURRENT_LOW': 'S32', 'DEA_L2_CURRENT_LOW': 'S32', 'DEA_L3_CURRENT_LOW': 'S32',
    'DEA_L1_VOLTAGE_LOW': 'S32', 'DEA_L2_VOLTAGE_LOW': 'S32', 'DEA_L3_VOLTAGE_LOW': 'S32',
    'DEA_TOTAL_ACTIVE_POWER_LOW': 'S32', 'DEA_TOTAL_REACTIVE_POWER_LOW': 'S32',
    'DEA_POWER_FACTOR_LOW': 'S32', 'DEA_FREQUENCY_LOW': 'S32', 'DEA_STATUS_FLAG_LOW': 'S32',
}

FLOAT32_FIELDS: set = set()
