#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Equipment Modbus Simulator (Multi-Slave)
Runs on PC with RS485 adapter - Single COM port, Multiple Slave IDs
Version: 1.4.0

Supported Devices:
- Inverter (Solarize): Slave ID 1, Holding Register (FC03)
- Protection Relay (KDU-300): Slave ID 2, Holding Register (FC03)
- Weather Station (SEM5046): Slave ID 3, Holding Register (FC03)
- Inverter (Kstar KSG-60KT-M1): Slave ID 4, Input Register (FC04)
- Inverter (Huawei SUN2000-50KTL): Slave ID 5, Holding Register (FC03)

Version 1.3.0:
- Added Huawei SUN2000-50KTL inverter simulator (Slave ID 5, FC03)

Version 1.2.8:
- Added register name display in Modbus log (e.g., 0x07D1 DER_ACTION_MODE)

Version 1.2.7:
- Fixed infinite recursion: set _internal_update flag before calling _update_registers()
- Set _internal_update during initialization to prevent control register callbacks

Version 1.2.6:
- Fixed Modbus log address display (shows actual register address, not internal offset)
- Log all external READ/WRITE operations for debugging

Version 1.2.5:
- Fixed pymodbus 3.7+ address offset issue using store.setValues()/getValues()
- Removed ADDR_OFFSET workaround, now uses proper Modbus API

Version 1.2.4:
- Fixed ModbusServerContext parameter (slaves -> devices for pymodbus 3.7+)

Version 1.2.3:
- Removed zero_mode parameter (deprecated in pymodbus 3.7+)

Version 1.2.2:
- Fixed pymodbus 3.7+ compatibility (ModbusSlaveContext -> ModbusDeviceContext)
- Fixed ModbusDeviceIdentification import path

Version 1.2.1:
- Fixed pymodbus import error handling (sys.exit on failure)

Version 1.2.0:
- Added SEM5046 weather station simulator
- Time-based solar radiation pattern (sunrise to sunset)
- Module temperature with variance

Version 1.1.0:
- Multi-device support on single COM port
- Inverter (Solarize) + Protection Relay (KDU-300) combined
- Unified Modbus server with multiple slave contexts
- Combined status display

Version 1.0.0:
- Initial release with KDU-300 only
"""

import sys
import os
import time
import math
import struct
import threading
import logging
import argparse
import random
import json
from datetime import datetime
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from pymodbus.server import StartSerialServer
    from pymodbus.datastore import ModbusServerContext, ModbusSequentialDataBlock
    from pymodbus import ModbusDeviceIdentification
    # pymodbus 3.7+ renamed ModbusSlaveContext to ModbusDeviceContext
    try:
        from pymodbus.datastore import ModbusSlaveContext
    except ImportError:
        from pymodbus.datastore import ModbusDeviceContext as ModbusSlaveContext
except ImportError as e:
    print(f"ERROR: pymodbus import failed: {e}")
    print("Install: pip install pymodbus")
    sys.exit(1)

from common.Solarize_PV_50kw_registers import (
    RegisterMap, InverterMode, SCALE,
    generate_iv_voltage_data, generate_iv_current_data,
    get_iv_tracker_voltage_registers, get_iv_string_current_registers
)

# IV Scan 상수 — 레지스터 파일에 없을 수 있으므로 시뮬레이터 자체 정의
try:
    from common.Solarize_PV_50kw_registers import IVScanCommand, IVScanStatus
except ImportError:
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
from common.REF_relay_registers import KDU300RegisterMap, float_to_registers
from common.REF_weather_registers import (
    SEM5046RegisterMap,
    air_temp_to_raw, humidity_to_raw, pressure_to_raw,
    wind_speed_to_raw, wind_direction_to_raw, module_temp_to_raw,
    accum_radiation_to_raw
)
from common.Kstar_PV_60kw_registers import RegisterMap as KstarRegisters
from common.Huawei_PV_50kw_registers import RegisterMap as HuaweiRegisters
try:
    from common.Huawei_PV_50kw_registers import HuaweiStatusConverter
except ImportError:
    from common.Huawei_PV_50kw_registers import HuaweiPvStatusConverter as HuaweiStatusConverter
try:
    from common.Ekos_PV_10kw_registers import RegisterMap as EkosRegisters, InverterMode as EkosInverterMode
except ImportError:
    EkosRegisters = None
    EkosInverterMode = None
try:
    from common.Sungrow_PV_50kw_registers import RegisterMap as SungrowRegisters, InverterMode as SungrowInverterMode
except ImportError:
    SungrowRegisters = None
    SungrowInverterMode = None


# =============================================================================
# Shared Solar Environment Model
# =============================================================================

class SolarEnvironment:
    """Shared realistic solar plant environment model.

    Provides time-of-day based radiation, temperature, humidity, wind, and
    cloud effects that all simulators can reference for consistent data.
    """

    def __init__(self):
        # Cloud effect state: random dips that persist 1-3 minutes
        self._cloud_factor = 1.0          # 0.85-1.0 (1.0 = clear)
        self._cloud_end_time = 0.0        # when current cloud event ends
        self._next_cloud_time = time.time() + random.uniform(60, 300)
        # Frequency random walk state
        self._freq_hz = 60.0
        # Lock for thread safety
        self._lock = threading.Lock()
        # Cached values (updated each tick)
        self.radiation = 0.0              # W/m2
        self.air_temp = 20.0              # C
        self.humidity = 60.0              # %
        self.wind_speed = 2.0             # m/s
        self.wind_direction = 180.0       # degrees
        self.module_temp = 20.0           # C
        self.frequency = 60.0             # Hz
        self.cloud_factor = 1.0           # current multiplier

    def update(self):
        """Call once per second from the update loop."""
        with self._lock:
            now = datetime.now()
            hour = now.hour + now.minute / 60.0 + now.second / 3600.0
            t = time.time()

            # --- Cloud effect ---
            if t >= self._cloud_end_time:
                # Cloud event finished
                self._cloud_factor = 1.0
            if t >= self._next_cloud_time and self._cloud_factor >= 0.99:
                # Start new cloud event
                self._cloud_factor = random.uniform(0.85, 0.95)
                duration = random.uniform(60, 180)  # 1-3 minutes
                self._cloud_end_time = t + duration
                self._next_cloud_time = t + duration + random.uniform(120, 600)
            self.cloud_factor = self._cloud_factor

            # --- Solar radiation: sun arc with cloud + noise ---
            sunrise, sunset = 5.5, 18.5
            if hour < sunrise or hour > sunset:
                base_radiation = 0.0
            else:
                day_frac = (hour - sunrise) / (sunset - sunrise)
                base_radiation = 1000.0 * math.sin(day_frac * math.pi)
            noise = 1.0 + random.uniform(-0.03, 0.03)
            self.radiation = max(0.0, base_radiation * self.cloud_factor * noise)

            # --- Air temperature: lags radiation by ~2hrs ---
            # Min ~15C at 05:00, max ~30C at 14:00
            temp_hour = hour - 14.0  # phase so peak is at 14:00
            self.air_temp = 22.5 + 7.5 * math.sin(temp_hour / 24.0 * 2 * math.pi)
            self.air_temp += random.uniform(-0.5, 0.5)

            # --- Humidity: inversely correlated with temperature ---
            # 80% at night low temp, 40% at midday high temp
            temp_norm = (self.air_temp - 15.0) / 15.0  # 0..1 range
            temp_norm = max(0.0, min(1.0, temp_norm))
            self.humidity = 80.0 - 40.0 * temp_norm + random.uniform(-3, 3)
            self.humidity = max(20.0, min(95.0, self.humidity))

            # --- Wind: slight increase in afternoon ---
            base_wind = 2.0
            if 12.0 <= hour <= 17.0:
                base_wind = 3.5
            self.wind_speed = max(0.0, base_wind + random.uniform(-1.5, 2.5))
            self.wind_direction += random.uniform(-5, 5)
            self.wind_direction = self.wind_direction % 360.0

            # --- Module temperature: air_temp + radiation/30 ---
            self.module_temp = self.air_temp + self.radiation / 30.0 + random.uniform(-1, 1)

            # --- Grid frequency: small random walk around 60Hz ---
            self._freq_hz += random.uniform(-0.005, 0.005)
            self._freq_hz = max(59.95, min(60.05, self._freq_hz))
            self.frequency = self._freq_hz

    def get_sun_fraction(self):
        """Return 0.0-1.0 sun fraction (radiation / 1000) for inverter scaling."""
        return max(0.0, min(1.0, self.radiation / 1000.0))

    def get_pv_voltage_factor(self):
        """PV voltage is higher in cold temperature, lower in hot.
        Returns multiplier around 1.0 based on module temperature."""
        # ~+0.3%/C below 25C, -0.3%/C above 25C (simplified)
        return 1.0 + (25.0 - self.module_temp) * 0.003


# Singleton environment — created by EquipmentSimulator and passed to all sims
_shared_env = None


def _get_shared_env():
    """Get or create the shared environment singleton."""
    global _shared_env
    if _shared_env is None:
        _shared_env = SolarEnvironment()
    return _shared_env


# =============================================================================
# Modbus Data Blocks with Logging
# =============================================================================

# Register address to name mapping for logging
REGISTER_NAMES = {
    # Device Info
    0x1A00: "DEVICE_MODEL",
    0x1A10: "SERIAL_NUMBER",
    0x1A1C: "MASTER_FW_VER",
    0x1A26: "SLAVE_FW_VER",
    0x1A3B: "MPPT_COUNT",
    0x1A44: "NOMINAL_VOLTAGE",
    0x1A45: "NOMINAL_FREQ",
    0x1A46: "NOMINAL_POWER_L",
    0x1A4E: "NOMINAL_POWER_H",
    0x1A48: "GRID_PHASE_NUM",
    0x1A60: "EMS_FW_VER",
    0x1A8E: "LCD_FW_VER",
    # Status
    0x101C: "INNER_TEMP",
    0x101D: "INVERTER_MODE",
    0x1021: "TOTAL_ENERGY_L",
    0x1022: "TOTAL_ENERGY_H",
    0x1027: "TODAY_ENERGY_L",
    0x1028: "TODAY_ENERGY_H",
    0x1037: "GRID_ACTIVE_PWR_L",
    0x1038: "GRID_ACTIVE_PWR_H",
    0x1039: "GRID_REACTIVE_L",
    0x103A: "GRID_REACTIVE_H",
    0x103D: "POWER_FACTOR",
    0x1048: "PV_INPUT_PWR_L",
    0x1049: "PV_INPUT_PWR_H",
    # DEA
    0x03F4: "DEA_ACTIVE_PWR_L",
    0x03F5: "DEA_ACTIVE_PWR_H",
    0x03F6: "DEA_REACTIVE_L",
    0x03F7: "DEA_REACTIVE_H",
    0x03F8: "DEA_PF_L",
    0x03F9: "DEA_PF_H",
    0x03FA: "DEA_FREQ_L",
    0x03FB: "DEA_FREQ_H",
    0x03FC: "DEA_STATUS_L",
    0x03FD: "DEA_STATUS_H",
    # DER Control
    0x07D0: "DER_PF_SET",
    0x07D1: "DER_ACTION_MODE",
    0x07D2: "DER_REACTIVE_PCT",
    0x07D3: "DER_ACTIVE_PCT",
    0x0834: "INVERTER_ON_OFF",
    # Commands
    0x6001: "INVERTER_CTRL",
    0x600D: "IV_CURVE_SCAN",
    0x600F: "PF_DYNAMIC",
    0x6010: "REACTIVE_DYNAMIC",
    0x3005: "POWER_DERATING",
    # IV Scan Status
    0x8000: "IV_SCAN_STATUS",
    0x8040: "IV_STRING1",
    0x8080: "IV_STRING2",
    0x8180: "IV_STRING3",
    0x81C0: "IV_STRING4",
    0x82C0: "IV_STRING5",
    0x8300: "IV_STRING6",
    0x8400: "IV_STRING7",
    0x8440: "IV_STRING8",
}

def get_register_name(addr):
    """Get register name for address, checking range for multi-register blocks"""
    if addr in REGISTER_NAMES:
        return REGISTER_NAMES[addr]
    # Check IV string ranges
    for base in [0x8040, 0x8080, 0x8180, 0x81C0, 0x82C0, 0x8300, 0x8400, 0x8440]:
        if base <= addr < base + 64:
            return f"IV_STR{([0x8040,0x8080,0x8180,0x81C0,0x82C0,0x8300,0x8400,0x8440].index(base)+1)}+{addr-base}"
    return ""

class ModbusLoggedHoldingBlock(ModbusSequentialDataBlock):
    """Modbus Holding Register Block with logging"""
    
    CONTROL_REGISTERS = [0x0834, 0x07D0, 0x07D1, 0x07D2, 0x07D3, 0x600D]
    IV_STRING_BASES = [0x8040, 0x8080, 0x8180, 0x81C0, 0x82C0, 0x8300, 0x8400, 0x8440]
    
    def __init__(self, address, values, logger=None, simulator=None, name="HR"):
        super().__init__(address, values)
        self.logger = logger
        self.log_queue = deque(maxlen=100)
        self._internal_update = False
        self._update_lock = threading.Lock()
        self.simulator = simulator
        self.name = name  # "INV" or "RLY" or "WTH"
    
    def getValues(self, address, count=1):
        result = super().getValues(address, count)
        # pymodbus 3.7+ internal offset: actual Modbus address = address - 1
        actual_addr = address - 1
        
        # Log all external reads (not internal updates)
        if self.logger and not self._internal_update:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            reg_name = get_register_name(actual_addr)
            name_str = f" {reg_name}" if reg_name else ""
            log_entry = f"[{timestamp}] {self.name} READ  0x{actual_addr:04X} x{count}{name_str}"
            self.log_queue.append(log_entry)
        
        # IV Scan tracking - only for InverterSimulator
        if self.simulator and hasattr(self.simulator, 'iv_scan_status'):
            if self.simulator.iv_scan_status == IVScanStatus.FINISHED:
                for i, base_addr in enumerate(self.IV_STRING_BASES):
                    if base_addr <= actual_addr < base_addr + 64:
                        self.simulator._iv_strings_read.add(i)
                        if len(self.simulator._iv_strings_read) >= 8:
                            self.simulator.iv_scan_status = IVScanStatus.IDLE
                            self.simulator._iv_strings_read.clear()
                        break
        return result
    
    def setValues(self, address, values):
        # pymodbus 3.7+ internal offset: actual Modbus address = address - 1
        actual_addr = address - 1
        is_control_write = actual_addr in self.CONTROL_REGISTERS and not self._internal_update
        
        # Log all external writes (not internal updates)
        if self.logger and isinstance(values, list) and not self._internal_update:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            reg_name = get_register_name(actual_addr)
            name_str = f" {reg_name}" if reg_name else ""
            if len(values) <= 10:
                log_entry = f"[{timestamp}] {self.name} WRITE 0x{actual_addr:04X} <- {values}{name_str}"
            else:
                log_entry = f"[{timestamp}] {self.name} WRITE 0x{actual_addr:04X} <- [{len(values)} regs]{name_str}"
            self.log_queue.append(log_entry)
        
        result = super().setValues(address, values)
        
        if is_control_write and self.simulator:
            # Prevent recursion: set flag before calling update methods
            with self._update_lock:
                self._internal_update = True
                try:
                    self.simulator._check_control_changes()
                    self.simulator._update_registers()
                finally:
                    self._internal_update = False
        
        return result


class ModbusLoggedInputBlock(ModbusSequentialDataBlock):
    """Modbus Input Register Block with logging for Relay"""
    
    def __init__(self, address, values, logger=None, simulator=None, name="IR"):
        super().__init__(address, values)
        self.logger = logger
        self.log_queue = deque(maxlen=100)
        self._internal_update = False
        self.simulator = simulator
        self.name = name
    
    def getValues(self, address, count=1):
        result = super().getValues(address, count)
        # pymodbus 3.7+ internal offset: actual Modbus address = address - 1
        actual_addr = address - 1
        if self.logger and not self._internal_update:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            log_entry = f"[{timestamp}] {self.name} READ  0x{actual_addr:04X} x{count}"
            self.log_queue.append(log_entry)
        return result
    
    def setValues(self, address, values):
        # pymodbus 3.7+ internal offset: actual Modbus address = address - 1
        actual_addr = address - 1
        if self.logger and isinstance(values, list) and not self._internal_update:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            if len(values) <= 10:
                log_entry = f"[{timestamp}] {self.name} WRITE 0x{actual_addr:04X} <- {values}"
            else:
                log_entry = f"[{timestamp}] {self.name} WRITE 0x{actual_addr:04X} <- [{len(values)} regs]"
            self.log_queue.append(log_entry)
        return super().setValues(address, values)


# =============================================================================
# Inverter Simulator (Solarize)
# =============================================================================

class RelaySimulator:
    """KDU-300 Protection Relay Simulator - Slave ID 2

    Realistic factory load pattern with weekday/weekend distinction.
    Uses Holding Register (FC03) for simulator compatibility.
    """

    VERSION = "1.2.0"
    NOMINAL_LINE_VOLTAGE = 380.0
    NOMINAL_PHASE_VOLTAGE = 220.0
    NOMINAL_FREQUENCY = 60.0
    NOMINAL_POWER = 300000.0       # 300kW factory peak
    NOMINAL_POWER_FACTOR = 0.93    # Industrial motors

    def __init__(self, logger=None, inverter_sims=None, env=None):
        self.logger = logger or logging.getLogger("RelaySim")
        self.running = False
        self.inverter_sims = inverter_sims or []
        self.env = env or _get_shared_env()

        self.start_time = time.time()
        self.received_energy_wh = 50000.0   # Grid import (+WH)
        self.sent_energy_wh = 0.0           # Grid export (-WH)
        self.total_energy_varh = 20000.0

        self.max_values = {
            'v12': 0.0, 'v23': 0.0, 'v31': 0.0,
            'v1': 0.0, 'v2': 0.0, 'v3': 0.0,
            'a1': 0.0, 'a2': 0.0, 'a3': 0.0,
            'w': 0.0
        }

        self.store = self._create_datastore()
        self._current = {}
    
    def _create_datastore(self):
        """Create Modbus datastore - Uses Holding Register (FC03)"""
        hr_block = ModbusLoggedHoldingBlock(
            0, [0] * KDU300RegisterMap.TOTAL_REGISTERS,
            self.logger, simulator=self, name="RLY"
        )
        
        store = ModbusSlaveContext(
            di=ModbusSequentialDataBlock(0, [0] * 100),
            co=ModbusSequentialDataBlock(0, [0] * 100),
            hr=hr_block,
            ir=ModbusSequentialDataBlock(0, [0] * 100)
        )
        
        return store
    
    def _get_total_inverter_power_w(self):
        """Get total AC power from all connected inverters (W)"""
        total = 0.0
        for sim in self.inverter_sims:
            if hasattr(sim, '_current') and sim._current:
                total += sim._current.get('ac_power_kw', 0.0) * 1000.0
        return total

    def _get_load_power_w(self, inverter_power_w):
        """Get factory load power (W) — realistic weekday/weekend pattern.

        Weekday (Mon-Fri):
          00:00-06:00: Base load 30kW (security, HVAC standby)
          06:00-08:00: Ramp up to 250kW (factory startup)
          08:00-12:00: Full load 280-300kW (production, +/-5%)
          12:00-13:00: Reduced 200kW (lunch break)
          13:00-18:00: Full load 280-300kW (afternoon production)
          18:00-20:00: Ramp down to 80kW (shutdown, cleaning)
          20:00-24:00: Low load 50kW (night shift minimal)

        Weekend (Sat-Sun):
          All day: Base load 30-40kW (security, HVAC, refrigeration)
        """
        now = datetime.now()
        hour = now.hour + now.minute / 60.0
        is_weekend = now.weekday() >= 5  # Saturday=5, Sunday=6

        if is_weekend:
            load_w = 35000.0 + random.uniform(-5000, 5000)
            return max(20000.0, load_w)

        # Weekday pattern
        fluctuation = random.uniform(-0.05, 0.05)
        if hour < 6.0:
            base_load = 30000.0
        elif hour < 8.0:
            # Ramp from 30kW to 250kW
            frac = (hour - 6.0) / 2.0
            base_load = 30000.0 + frac * 220000.0
        elif hour < 12.0:
            # Full production 280-300kW
            base_load = 290000.0
        elif hour < 13.0:
            # Lunch break reduced to 200kW
            base_load = 200000.0
        elif hour < 18.0:
            # Afternoon full production 280-300kW
            base_load = 290000.0
        elif hour < 20.0:
            # Ramp down from 290kW to 80kW
            frac = (hour - 18.0) / 2.0
            base_load = 290000.0 - frac * 210000.0
        else:
            # Night shift minimal 50kW
            base_load = 50000.0

        return max(20000.0, base_load * (1.0 + fluctuation))
    
    def _update_registers(self):
        """Update all register values — PCC net power model"""
        env = self.env
        # Get inverter generation and factory load
        inverter_power_w = self._get_total_inverter_power_w()
        load_power_w = self._get_load_power_w(inverter_power_w)

        # Net power at PCC = Load - Inverter Generation
        # Positive = consuming from grid, Negative = exporting to grid
        net_power_w = load_power_w - inverter_power_w

        # Industrial power factor: 0.92-0.95
        pf = self.NOMINAL_POWER_FACTOR + random.uniform(-0.015, 0.015)

        # Voltage from shared environment
        v_base = self.NOMINAL_LINE_VOLTAGE * (1.0 + random.uniform(-0.02, 0.02))
        v12 = v_base * (1.0 + random.uniform(-0.01, 0.01))
        v23 = v_base * (1.0 + random.uniform(-0.01, 0.01))
        v31 = v_base * (1.0 + random.uniform(-0.01, 0.01))

        v1 = v12 / math.sqrt(3) * (1.0 + random.uniform(-0.005, 0.005))
        v2 = v23 / math.sqrt(3) * (1.0 + random.uniform(-0.005, 0.005))
        v3 = v31 / math.sqrt(3) * (1.0 + random.uniform(-0.005, 0.005))

        freq = env.frequency

        # Power — net_power_w can be negative (export to grid)
        total_w = net_power_w
        w1 = total_w / 3 * (1.0 + random.uniform(-0.03, 0.03))
        w2 = total_w / 3 * (1.0 + random.uniform(-0.03, 0.03))
        w3 = total_w - w1 - w2
        
        pf_angle = math.acos(pf)
        total_reactive = total_w * math.tan(pf_angle)
        var1 = total_reactive / 3 * (1.0 + random.uniform(-0.03, 0.03))
        var2 = total_reactive / 3 * (1.0 + random.uniform(-0.03, 0.03))
        var3 = total_reactive - var1 - var2
        total_var = var1 + var2 + var3
        
        va1 = math.sqrt(w1**2 + var1**2)
        va2 = math.sqrt(w2**2 + var2**2)
        va3 = math.sqrt(w3**2 + var3**2)
        total_va = math.sqrt(total_w**2 + total_var**2)
        
        a1 = va1 / v1 if v1 > 0 else 0
        a2 = va2 / v2 if v2 > 0 else 0
        a3 = va3 / v3 if v3 > 0 else 0
        
        pf1 = w1 / va1 if va1 > 0 else 1.0
        pf2 = w2 / va2 if va2 > 0 else 1.0
        pf3 = w3 / va3 if va3 > 0 else 1.0
        avg_pf = total_w / total_va if total_va > 0 else 1.0
        
        p1_angle = math.degrees(math.acos(min(pf1, 1.0)))
        p2_angle = math.degrees(math.acos(min(pf2, 1.0)))
        p3_angle = math.degrees(math.acos(min(pf3, 1.0)))
        
        # Energy accumulation based on power direction
        energy_delta = abs(total_w) * (1.0 / 3600.0)  # 1s update interval
        if total_w >= 0:
            self.received_energy_wh += energy_delta   # 수전 (+WH)
        else:
            self.sent_energy_wh += energy_delta        # 역송 (-WH)
        self.total_energy_varh += abs(total_var) * (1.0 / 3600.0)
        
        # Update max values
        self.max_values['v12'] = max(self.max_values['v12'], v12)
        self.max_values['v23'] = max(self.max_values['v23'], v23)
        self.max_values['v31'] = max(self.max_values['v31'], v31)
        self.max_values['v1'] = max(self.max_values['v1'], v1)
        self.max_values['v2'] = max(self.max_values['v2'], v2)
        self.max_values['v3'] = max(self.max_values['v3'], v3)
        self.max_values['a1'] = max(self.max_values['a1'], a1)
        self.max_values['a2'] = max(self.max_values['a2'], a2)
        self.max_values['a3'] = max(self.max_values['a3'], a3)
        self.max_values['w'] = max(self.max_values['w'], total_w)
        
        def set_float(addr, value):
            hi, lo = float_to_registers(value)
            self.store.setValues(3, addr, [hi, lo])
        
        # Write all float values
        set_float(KDU300RegisterMap.V12, v12)
        set_float(KDU300RegisterMap.V23, v23)
        set_float(KDU300RegisterMap.V31, v31)
        set_float(KDU300RegisterMap.V1, v1)
        set_float(KDU300RegisterMap.V2, v2)
        set_float(KDU300RegisterMap.V3, v3)
        set_float(KDU300RegisterMap.A1, a1)
        set_float(KDU300RegisterMap.A2, a2)
        set_float(KDU300RegisterMap.A3, a3)
        set_float(KDU300RegisterMap.W1, w1)
        set_float(KDU300RegisterMap.W2, w2)
        set_float(KDU300RegisterMap.W3, w3)
        set_float(KDU300RegisterMap.TOTAL_W, total_w)
        set_float(KDU300RegisterMap.VAR1, var1)
        set_float(KDU300RegisterMap.VAR2, var2)
        set_float(KDU300RegisterMap.VAR3, var3)
        set_float(KDU300RegisterMap.TOTAL_VAR, total_var)
        set_float(KDU300RegisterMap.VA1, va1)
        set_float(KDU300RegisterMap.VA2, va2)
        set_float(KDU300RegisterMap.VA3, va3)
        set_float(KDU300RegisterMap.TOTAL_VA, total_va)
        set_float(KDU300RegisterMap.PF1, pf1)
        set_float(KDU300RegisterMap.PF2, pf2)
        set_float(KDU300RegisterMap.PF3, pf3)
        set_float(KDU300RegisterMap.AVG_PF, avg_pf)
        set_float(KDU300RegisterMap.FREQUENCY, freq)
        set_float(KDU300RegisterMap.POSITIVE_WH, self.received_energy_wh)
        set_float(KDU300RegisterMap.NEGATIVE_WH, self.sent_energy_wh)
        set_float(KDU300RegisterMap.POSITIVE_VARH, self.total_energy_varh)
        set_float(KDU300RegisterMap.NEGATIVE_VARH, 0.0)
        set_float(KDU300RegisterMap.V12_MAX, self.max_values['v12'])
        set_float(KDU300RegisterMap.V23_MAX, self.max_values['v23'])
        set_float(KDU300RegisterMap.V31_MAX, self.max_values['v31'])
        set_float(KDU300RegisterMap.V1_MAX, self.max_values['v1'])
        set_float(KDU300RegisterMap.V2_MAX, self.max_values['v2'])
        set_float(KDU300RegisterMap.V3_MAX, self.max_values['v3'])
        set_float(KDU300RegisterMap.A1_MAX, self.max_values['a1'])
        set_float(KDU300RegisterMap.A2_MAX, self.max_values['a2'])
        set_float(KDU300RegisterMap.A3_MAX, self.max_values['a3'])
        set_float(KDU300RegisterMap.W_MAX, self.max_values['w'])
        set_float(KDU300RegisterMap.P1_ANGLE, p1_angle)
        set_float(KDU300RegisterMap.P2_ANGLE, p2_angle)
        set_float(KDU300RegisterMap.P3_ANGLE, p3_angle)
        set_float(KDU300RegisterMap.REVERSE_WATT1, 0.0)
        set_float(KDU300RegisterMap.REVERSE_WATT2, 0.0)
        set_float(KDU300RegisterMap.REVERSE_WATT3, 0.0)
        
        # DO status: 0x0001 if exporting to grid (reverse power), 0x0000 if consuming
        do_status = 0x0001 if total_w < 0 else 0x0000
        self.store.setValues(3, KDU300RegisterMap.DO_STATUS, [do_status])
        self.store.setValues(3, KDU300RegisterMap.OVR, [0, 0, 0, 0, 0])  # OVR, UVR, OFR, UFR, RPR
        self.store.setValues(3, KDU300RegisterMap.DI1, [do_status, 0x0000])  # DI1=역전력차단기 접점, DI2
        
        # Store for display
        self._current = {
            'inverter_kw': inverter_power_w / 1000,
            'load_kw': load_power_w / 1000,
            'net_kw': total_w / 1000,
            'total_var': total_var / 1000,
            'avg_pf': avg_pf,
            'freq': freq,
            'v12': v12,
            'a1': a1,
            'do_status': do_status,
            'received_kwh': self.received_energy_wh / 1000,
            'sent_kwh': self.sent_energy_wh / 1000,
        }


# =============================================================================
# Weather Station Simulator (SEM5046)
# =============================================================================

class WeatherSimulator:
    """SEM5046 Weather Station Simulator - Slave ID 3

    Realistic daily patterns from shared SolarEnvironment.
    """

    VERSION = "1.1.0"

    def __init__(self, logger=None, env=None):
        self.logger = logger or logging.getLogger("WeatherSim")
        self.running = False
        self.env = env or _get_shared_env()

        self.start_time = time.time()
        self.accum_horizontal = 0.0  # MJ/m²
        self.accum_inclined = 0.0    # MJ/m²

        self.store = self._create_datastore()
        self._current = {}

    def _create_datastore(self):
        """Create Modbus datastore - Uses Holding Register (FC03)"""
        hr_block = ModbusLoggedHoldingBlock(
            0, [0] * SEM5046RegisterMap.TOTAL_REGISTERS,
            self.logger, simulator=self, name="WTH"
        )

        store = ModbusSlaveContext(
            di=ModbusSequentialDataBlock(0, [0] * 100),
            co=ModbusSequentialDataBlock(0, [0] * 100),
            hr=hr_block,
            ir=ModbusSequentialDataBlock(0, [0] * 100)
        )

        return store

    def _update_registers(self):
        """Update all register values from shared environment."""
        env = self.env
        radiation = env.radiation
        air_temp = env.air_temp
        humidity = env.humidity
        pressure = 1013.0 + random.uniform(-3, 3)
        wind_speed = env.wind_speed
        wind_direction = env.wind_direction

        # Module temperatures (4 sensors with small variance)
        base_module_temp = env.module_temp
        module_temp_1 = base_module_temp + random.uniform(-1.5, 1.5)
        module_temp_2 = base_module_temp + random.uniform(-1.5, 1.5)
        module_temp_3 = base_module_temp + random.uniform(-1.5, 1.5)
        module_temp_4 = base_module_temp + random.uniform(-1.5, 1.5)

        # Inclined radiation (tilted panel, ~15% more when sun is up)
        inclined_factor = 1.15 if radiation > 0 else 1.0
        inclined_radiation = radiation * inclined_factor

        # Accumulate radiation (every 1 second update)
        # W/m² * 1s = Ws/m² -> /1000000 = MJ/m²
        self.accum_horizontal += radiation * 1.0 / 1000000.0
        self.accum_inclined += inclined_radiation * 1.0 / 1000000.0

        # Write to registers using store.setValues()
        self.store.setValues(3, SEM5046RegisterMap.AIR_TEMP, [air_temp_to_raw(air_temp)])
        self.store.setValues(3, SEM5046RegisterMap.AIR_HUMIDITY, [humidity_to_raw(humidity)])
        self.store.setValues(3, SEM5046RegisterMap.AIR_PRESSURE, [pressure_to_raw(pressure)])
        self.store.setValues(3, SEM5046RegisterMap.WIND_SPEED, [wind_speed_to_raw(wind_speed)])
        self.store.setValues(3, SEM5046RegisterMap.WIND_DIRECTION, [wind_direction_to_raw(wind_direction)])
        self.store.setValues(3, SEM5046RegisterMap.MODULE_TEMP_1, [module_temp_to_raw(module_temp_1)])
        self.store.setValues(3, SEM5046RegisterMap.HORIZONTAL_RADIATION, [int(radiation)])
        self.store.setValues(3, SEM5046RegisterMap.HORIZONTAL_ACCUM, [accum_radiation_to_raw(self.accum_horizontal)])
        self.store.setValues(3, SEM5046RegisterMap.INCLINED_RADIATION, [int(inclined_radiation)])
        self.store.setValues(3, SEM5046RegisterMap.INCLINED_ACCUM, [accum_radiation_to_raw(self.accum_inclined)])
        self.store.setValues(3, SEM5046RegisterMap.MODULE_TEMP_2, [module_temp_to_raw(module_temp_2)])
        self.store.setValues(3, SEM5046RegisterMap.MODULE_TEMP_3, [module_temp_to_raw(module_temp_3)])
        self.store.setValues(3, SEM5046RegisterMap.MODULE_TEMP_4, [module_temp_to_raw(module_temp_4)])

        # Store for display
        self._current = {
            'radiation': radiation,
            'inclined': inclined_radiation,
            'air_temp': air_temp,
            'humidity': humidity,
            'module_temp': module_temp_1,
            'wind_speed': wind_speed,
            'accum_h': self.accum_horizontal,
            'accum_i': self.accum_inclined
        }


# =============================================================================
# Kstar Inverter Simulator (KSG-60KT-M1)
# =============================================================================

class _KstarNightOffBlock(ModbusSequentialDataBlock):
    """FC04 Input Register block that refuses reads when inverter is powered off (night)."""

    def __init__(self, address, values):
        super().__init__(address, values)
        self.night_off = False

    def getValues(self, address, count=1):
        if self.night_off:
            # Return None to trigger IllegalAddress error in pymodbus
            raise Exception("Kstar inverter powered off (night)")
        return super().getValues(address, count)


class GenericInverterSimulator:
    """Generic Inverter Simulator — dynamically loads any *_registers.py module.

    Supports all Model Maker v2 generated register files. Falls back gracefully
    when optional attributes (INVERTER_MODE, STRING*_VOLTAGE, etc.) are missing.
    """

    VERSION = "1.0.0"
    NOMINAL_POWER = 50000  # 50kW default

    def __init__(self, protocol_name, logger=None, env=None):
        self.protocol_name = protocol_name
        self.logger = logger or logging.getLogger(f"Generic-{protocol_name}")
        self.running = False
        self.env = env or _get_shared_env()

        # Load register module dynamically
        self._module = self._load_module(protocol_name)
        self.reg_map = getattr(self._module, 'RegisterMap', None)
        self.inv_mode_cls = getattr(self._module, 'InverterMode', None)
        self.scale = getattr(self._module, 'SCALE', {})
        self.mppt_channels = getattr(self._module, 'MPPT_CHANNELS', 4)
        self.string_channels = getattr(self._module, 'STRING_CHANNELS', 0)
        self._get_mppt_regs = getattr(self._module, 'get_mppt_registers', None)
        self._get_string_regs = getattr(self._module, 'get_string_registers', None)

        # Use FC03 by default (no register file uses RTU_FC_CODE currently)
        self.fc_code = getattr(self._module, 'RTU_FC_CODE', 3)

        # Simulation state
        self.start_time = time.time()
        self.total_energy = 1000.0  # kWh
        self.on_off = 0
        self.power_limit = 1000
        self.power_factor_set = 1000
        self.reactive_power_set = 0
        self.control_mode = 'PF'
        self.operation_mode = 0

        # Resolve scale factors
        self._s_voltage = self.scale.get('voltage', 0.1)
        self._s_current = self.scale.get('current', 0.01)
        self._s_power = self.scale.get('power', 0.1)
        self._s_freq = self.scale.get('frequency', 0.01)

        # PV nominal voltages per MPPT (0.1V units → ~390V)
        self._pv_v_nom = [3900 + i * 50 for i in range(self.mppt_channels)]

        self.store = self._create_datastore()
        self._current = {}
        self._current_lock = threading.Lock()

        self.logger.info(f"[GENERIC] Loaded protocol '{protocol_name}' | "
                         f"MPPT={self.mppt_channels} STR={self.string_channels} FC={self.fc_code:02d}")

    @staticmethod
    def _load_module(protocol_name):
        """Load common/{protocol}_registers.py using same logic as RTU modbus_handler."""
        import importlib, glob as _glob

        common_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common')

        # 1st: exact name
        module_name = f"common.{protocol_name}_registers"
        try:
            return importlib.import_module(module_name)
        except ImportError:
            pass

        # 2nd: case-insensitive prefix glob
        prefix = protocol_name.split('_')[0].lower()
        candidates = sorted(_glob.glob(os.path.join(common_dir, '*_registers.py')))
        for fpath in candidates:
            fname = os.path.basename(fpath)
            if fname.lower().startswith(prefix) and not fname.startswith('REF_'):
                mod_name = f"common.{fname[:-3]}"
                try:
                    return importlib.import_module(mod_name)
                except ImportError:
                    continue

        raise ImportError(f"Register module for protocol '{protocol_name}' not found in {common_dir}")

    def _find_addr(self, *names):
        """Try multiple attribute names on RegisterMap, return first valid int address or None."""
        if self.reg_map is None:
            return None
        for name in names:
            addr = getattr(self.reg_map, name, None)
            if addr is not None and isinstance(addr, int):
                return addr
        return None

    def _create_datastore(self):
        """Create Modbus datastore — FC03 holding register or FC04 input register."""
        block = ModbusLoggedHoldingBlock(
            0, [0] * 0x8500, self.logger, simulator=self,
            name=self.protocol_name[:3].upper()
        )

        if self.fc_code == 4:
            store = ModbusSlaveContext(
                di=ModbusSequentialDataBlock(0, [0] * 10),
                co=ModbusSequentialDataBlock(0, [0] * 10),
                hr=ModbusSequentialDataBlock(0, [0] * 10),
                ir=block,
            )
        else:
            store = ModbusSlaveContext(
                di=ModbusSequentialDataBlock(0, [0] * 10),
                co=ModbusSequentialDataBlock(0, [0] * 10),
                hr=block,
                ir=ModbusSequentialDataBlock(0, [0] * 10),
            )

        self.store = store
        block._internal_update = True
        self._init_control_registers()
        block._internal_update = False
        return store

    def _init_control_registers(self):
        """Set DER-AVM control register initial values."""
        fc = self.fc_code + 1 if self.fc_code == 4 else 3  # FC04→fc_as_hex=4, FC03→3
        addr = self._find_addr('DER_POWER_FACTOR_SET')
        if addr is not None:
            self.store.setValues(fc, addr, [self.power_factor_set])
        addr = self._find_addr('DER_ACTION_MODE')
        if addr is not None:
            self.store.setValues(fc, addr, [self.operation_mode])
        addr = self._find_addr('DER_REACTIVE_POWER_PCT')
        if addr is not None:
            self.store.setValues(fc, addr, [self.reactive_power_set])
        addr = self._find_addr('DER_ACTIVE_POWER_PCT')
        if addr is not None:
            self.store.setValues(fc, addr, [self.power_limit])
        addr = self._find_addr('INVERTER_ON_OFF')
        if addr is not None:
            self.store.setValues(fc, addr, [self.on_off])

    def _get_sun_factor(self):
        """Get sun factor from shared environment."""
        return self.env.get_sun_fraction()

    def _set_reg(self, addr, values):
        """Write values to the correct function code store."""
        if addr is None:
            return
        fc = self.fc_code + 1 if self.fc_code == 4 else 3
        self.store.setValues(fc, addr, values)

    def _get_reg(self, addr, count=1):
        """Read values from the correct function code store."""
        if addr is None:
            return [0] * count
        fc = self.fc_code + 1 if self.fc_code == 4 else 3
        return self.store.getValues(fc, addr, count=count)

    def _write_u32(self, low_addr, value):
        """Write a U32 value respecting U32_WORD_ORDER from register module."""
        if low_addr is None:
            return
        val = int(value) & 0xFFFFFFFF
        lo_word = val & 0xFFFF
        hi_word = (val >> 16) & 0xFFFF
        if getattr(self._module, 'U32_WORD_ORDER', 'LH') == 'HL':
            self._set_reg(low_addr, [hi_word])
            self._set_reg(low_addr + 1, [lo_word])
        else:
            self._set_reg(low_addr, [lo_word])
            self._set_reg(low_addr + 1, [hi_word])

    def _update_registers(self):
        """Update all register values — called every ~1s by EquipmentSimulator."""
        env = self.env
        sun = self._get_sun_factor()
        sun_var = sun
        pv_v_factor = env.get_pv_voltage_factor()

        EFFICIENCY = 0.97
        power_cap = self.NOMINAL_POWER * (self.power_limit / 1000.0)

        if sun_var > 0.01 and self.on_off == 0:
            possible_ac_w = sun_var * self.NOMINAL_POWER * EFFICIENCY
            ac_power_w = min(possible_ac_w, power_cap)
            pv_total_w = ac_power_w / EFFICIENCY
        else:
            ac_power_w = 0
            pv_total_w = 0

        # --- MPPT data ---
        n_mppt = self.mppt_channels or 4
        for i in range(n_mppt):
            if pv_total_w > 0:
                mppt_v_raw = int(self._pv_v_nom[i] * pv_v_factor * (0.92 + 0.08 * sun_var)
                                 + random.uniform(-10, 10))
                mppt_p_w = pv_total_w / n_mppt * random.uniform(0.98, 1.02)
                mppt_c_raw = int(mppt_p_w / (mppt_v_raw * self._s_voltage)
                                 / self._s_current) if mppt_v_raw > 0 else 0
                mppt_p_raw = int(mppt_p_w / self._s_power)
            else:
                mppt_v_raw, mppt_c_raw, mppt_p_raw = 0, 0, 0

            if self._get_mppt_regs:
                try:
                    regs = self._get_mppt_regs(i + 1)
                    # (voltage, current, power_low, power_high)
                    self._set_reg(regs[0], [mppt_v_raw])
                    self._set_reg(regs[1], [mppt_c_raw])
                    self._set_reg(regs[2], [mppt_p_raw & 0xFFFF])
                    self._set_reg(regs[3], [(mppt_p_raw >> 16) & 0xFFFF])
                except (ValueError, IndexError):
                    pass
            else:
                # Scan RegisterMap for MPPTn_VOLTAGE/CURRENT/POWER
                n = i + 1
                v_addr = self._find_addr(f'MPPT{n}_VOLTAGE', f'PV{n}_VOLTAGE')
                c_addr = self._find_addr(f'MPPT{n}_CURRENT', f'PV{n}_CURRENT')
                p_addr = self._find_addr(f'MPPT{n}_POWER')
                self._set_reg(v_addr, [mppt_v_raw])
                self._set_reg(c_addr, [mppt_c_raw])
                if p_addr is not None:
                    self._set_reg(p_addr, [mppt_p_raw & 0xFFFF])
                    self._set_reg(p_addr + 1, [(mppt_p_raw >> 16) & 0xFFFF])

        # --- String data ---
        for i in range(self.string_channels):
            mppt_idx = i // max(1, self.string_channels // max(1, n_mppt))
            mppt_idx = min(mppt_idx, n_mppt - 1)
            if pv_total_w > 0:
                str_v_raw = int(self._pv_v_nom[mppt_idx] * pv_v_factor * (0.92 + 0.08 * sun_var)
                                + random.uniform(-5, 5))
                str_c_raw = int((pv_total_w / n_mppt / max(1, self.string_channels // n_mppt))
                                / (str_v_raw * self._s_voltage) / self._s_current) if str_v_raw > 0 else 0
            else:
                str_v_raw, str_c_raw = 0, 0

            if self._get_string_regs:
                try:
                    v_addr, c_addr = self._get_string_regs(i + 1)
                    self._set_reg(v_addr, [str_v_raw])
                    self._set_reg(c_addr, [str_c_raw])
                except (ValueError, IndexError):
                    pass
            else:
                n = i + 1
                v_addr = self._find_addr(f'STRING{n}_VOLTAGE')
                c_addr = self._find_addr(f'STRING{n}_CURRENT')
                self._set_reg(v_addr, [str_v_raw])
                self._set_reg(c_addr, [str_c_raw])

        # --- AC phase data ---
        ac_voltage_raw = int(380.0 / self._s_voltage) if ac_power_w > 0 else 0
        ac_freq_raw = int(env.frequency / self._s_freq)
        ac_power_raw = int(ac_power_w / self._s_power)
        pv_power_raw = int(pv_total_w / self._s_power)

        # Phase voltages (try multiple naming conventions)
        for names in [('L1_VOLTAGE', 'R_PHASE_VOLTAGE', 'A_PHASE_VOLTAGE'),
                      ('L2_VOLTAGE', 'S_PHASE_VOLTAGE', 'B_PHASE_VOLTAGE'),
                      ('L3_VOLTAGE', 'T_PHASE_VOLTAGE', 'C_PHASE_VOLTAGE')]:
            addr = self._find_addr(*names)
            self._set_reg(addr, [ac_voltage_raw])

        # Phase currents
        apparent_w = math.sqrt(ac_power_w**2) if ac_power_w > 0 else 0
        phase_current_raw = int(apparent_w / 3 / 380.0 / self._s_current) if apparent_w > 0 else 0
        for names in [('L1_CURRENT', 'R_PHASE_CURRENT'),
                      ('L2_CURRENT', 'S_PHASE_CURRENT'),
                      ('L3_CURRENT', 'T_PHASE_CURRENT')]:
            addr = self._find_addr(*names)
            self._set_reg(addr, [phase_current_raw])

        # Frequency
        addr = self._find_addr('FREQUENCY')
        self._set_reg(addr, [ac_freq_raw])

        # AC Power (U32)
        addr = self._find_addr('AC_POWER', 'ACTIVE_POWER')
        if addr is not None:
            self._write_u32(addr, ac_power_raw)

        # PV Power (U32)
        addr = self._find_addr('PV_POWER')
        if addr is not None:
            self._write_u32(addr, pv_power_raw)

        # Power factor
        pf = self.power_factor_set / 1000.0
        pf = max(0.85, min(1.0, abs(pf)))
        pf_raw = int(pf * 1000)
        addr = self._find_addr('POWER_FACTOR')
        self._set_reg(addr, [pf_raw & 0xFFFF])

        # Cumulative energy
        total_kwh = int(self.total_energy)
        addr = self._find_addr('CUMULATIVE_ENERGY', 'CUMULATIVE_ENERGY_LOW', 'TOTAL_ENERGY')
        if addr is not None:
            self._write_u32(addr, total_kwh)

        # Accumulate energy
        if ac_power_w > 0:
            self.total_energy += ac_power_w / 3600000.0  # W·s → kWh

        # Inverter mode
        addr = self._find_addr('INVERTER_MODE')
        if addr is not None:
            if self.inv_mode_cls:
                mode_val = getattr(self.inv_mode_cls, 'STANDBY', 1) if self.on_off == 1 \
                    else getattr(self.inv_mode_cls, 'ON_GRID', 3)
            else:
                mode_val = 1 if self.on_off == 1 else 3
            self._set_reg(addr, [mode_val])

        # Error codes
        for name in ('ERROR_CODE1', 'ERROR_CODE2', 'ERROR_CODE3'):
            addr = self._find_addr(name)
            self._set_reg(addr, [0])

        # Temperature — from environment
        inner_temp = int(env.air_temp + sun * 25.0 + random.uniform(-2, 2))
        addr = self._find_addr('INNER_TEMP', 'INTERNALTEMPERATURE', 'TEMPERATURE',
                               'INVERTER_INNERTEMPERATURE', 'INVERTER_MODULETEMPERATURE')
        self._set_reg(addr, [max(0, inner_temp)])

        # Update display dict (thread-safe)
        with self._current_lock:
            self._current = {
                'sun_factor': sun,
                'pv_power_kw': pv_total_w / 1000.0,
                'ac_power_kw': ac_power_w / 1000.0,
                'voltage': 380.0 if ac_power_w > 0 else 0,
                'on_off': 'ON' if self.on_off == 0 else 'OFF',
                'status': 'Running' if (self.on_off == 0 and sun_var > 0.01) else (
                    'OFF' if self.on_off == 1 else 'Standby'),
                'ctrl_mode': self.control_mode,
            }

    def _check_control_changes(self):
        """Poll DER-AVM control registers for external writes."""
        addr = self._find_addr('INVERTER_ON_OFF')
        if addr is not None:
            val = self._get_reg(addr)[0]
            if val != self.on_off and val in (0, 1):
                self.on_off = val
                self.logger.info(f"[{self.protocol_name}] ON/OFF -> {'OFF' if val else 'ON'}")

        addr = self._find_addr('DER_ACTIVE_POWER_PCT')
        if addr is not None:
            val = self._get_reg(addr)[0]
            if val != self.power_limit and 0 <= val <= 1100:
                self.power_limit = val
                self.logger.info(f"[{self.protocol_name}] Power limit -> {val/10:.1f}%")

        addr = self._find_addr('DER_POWER_FACTOR_SET')
        if addr is not None:
            val = self._get_reg(addr)[0]
            if val != self.power_factor_set:
                self.power_factor_set = val
                self.control_mode = 'PF'
                self.logger.info(f"[{self.protocol_name}] PF -> {val/1000:.3f}")

        addr = self._find_addr('DER_REACTIVE_POWER_PCT')
        if addr is not None:
            val = self._get_reg(addr)[0]
            if val != self.reactive_power_set:
                self.reactive_power_set = val
                self.control_mode = 'RP'
                self.logger.info(f"[{self.protocol_name}] Reactive -> {val/10:.1f}%")

        addr = self._find_addr('DER_ACTION_MODE')
        if addr is not None:
            val = self._get_reg(addr)[0]
            if val != self.operation_mode:
                self.operation_mode = val


# =============================================================================
# =============================================================================
# Broadcast Proxy (slave_id=0)
# =============================================================================

class _BroadcastProxy:
    """Proxy that forwards write operations to all inverter stores.
    Modbus broadcast (slave 0) sends write commands to all devices,
    no response is expected. Read operations return empty data."""

    def __init__(self, stores: list, logger=None):
        self.stores = stores  # list of ModbusLoggedBlock (inverter stores)
        self.logger = logger
        # Minimal SlaveContext-compatible interface
        self.store = self

    def validate(self, *args, **kwargs):
        return True

    def getValues(self, fc_as_hex, address, count=1):
        """Read from first store (broadcast reads are unusual but handle gracefully)."""
        if self.stores:
            return self.stores[0].getValues(address, count)
        return [0] * count

    def setValues(self, fc_as_hex, address, values):
        """Forward write to ALL inverter stores."""
        if self.logger:
            self.logger.info(f"[BROADCAST] Write 0x{address-1:04X} <- {values} to {len(self.stores)} inverters")
        for store in self.stores:
            store.setValues(address, values)


# =============================================================================
# Multi-Device Server
# =============================================================================

class EquipmentSimulator:
    """Multi-Device Equipment Simulator — Dynamic device configuration"""

    VERSION = "1.5.0"

    def __init__(self, config: dict):
        self.port = config.get('port', 'COM10')
        self.baudrate = config.get('baudrate', 9600)
        self.device_config = config.get('devices', [])
        self.running = False
        self.logger = logging.getLogger("EquipSim")

        # Shared environment for all simulators
        global _shared_env
        _shared_env = SolarEnvironment()
        self.env = _shared_env

        # Dynamic device creation
        self.devices = []  # list of (slave_id, type, name, protocol, simulator)
        device_map = {}    # slave_id -> store

        # First pass: create inverters and weather
        relay_configs = []
        for dc in self.device_config:
            sid = dc['slave_id']
            dtype = dc['type']
            name = dc['name']
            proto = dc.get('protocol', '')

            if dtype == 'inverter':
                sim = _create_inverter_by_protocol(proto, self.logger, env=self.env)
            elif dtype == 'relay':
                relay_configs.append(dc)
                continue  # Create relay after all inverters
            elif dtype == 'weather':
                sim = WeatherSimulator(self.logger, env=self.env)
            else:
                continue

            self.devices.append({
                'slave_id': sid,
                'type': dtype,
                'name': name,
                'protocol': proto,
                'sim': sim,
            })
            device_map[sid] = sim.store

        # Second pass: create relays with inverter references
        inverter_sims = [d['sim'] for d in self.devices if d['type'] == 'inverter']
        for dc in relay_configs:
            sid = dc['slave_id']
            name = dc['name']
            proto = dc.get('protocol', '')
            sim = RelaySimulator(self.logger, inverter_sims=inverter_sims, env=self.env)
            self.devices.append({
                'slave_id': sid,
                'type': 'relay',
                'name': name,
                'protocol': proto,
                'sim': sim,
            })
            device_map[sid] = sim.store

        # Broadcast support: slave_id=0 writes are forwarded to all inverters
        inverter_stores = [d['sim'].store for d in self.devices if d['type'] == 'inverter']
        if inverter_stores:
            device_map[0] = _BroadcastProxy(inverter_stores, self.logger)

        self.context = ModbusServerContext(devices=device_map, single=False)
    
    def _update_loop(self):
        """Background thread for updating all devices"""
        error_count = 0
        last_error_time = 0.0
        ERROR_RESET_INTERVAL = 60
        while self.running:
            try:
                # Update shared environment first (once per tick)
                self.env.update()
                for d in self.devices:
                    sim = d['sim']
                    sim._update_registers()
                    if hasattr(sim, '_check_control_changes'):
                        sim._check_control_changes()
                error_count = 0
            except Exception as e:
                now = time.time()
                if error_count > 0 and (now - last_error_time) > ERROR_RESET_INTERVAL:
                    self.logger.info(f"Error counter reset after {ERROR_RESET_INTERVAL}s of inactivity")
                    error_count = 0
                error_count += 1
                last_error_time = now
                self.logger.exception(f"Update error ({error_count}): {e}")
                if error_count >= 10:
                    self.logger.critical("Too many consecutive errors, halting simulator")
                    self.running = False
                    break
            time.sleep(1)
    
    def _display_status(self):
        """Display combined status for all configured devices"""
        while self.running:
            try:
                timestamp = datetime.now().strftime("%H:%M:%S")

                # Wait for all devices to have data
                all_ready = all(d['sim']._current for d in self.devices)
                if not all_ready:
                    time.sleep(1)
                    continue

                if not self.running:
                    break

                print("\033[2J\033[H", end="")
                print("=" * 80)
                print(f"  Equipment Simulator v{self.VERSION} - {timestamp}")
                print(f"  Port: {self.port} | Baud: {self.baudrate} | Devices: {len(self.devices)}")
                print("=" * 80)

                for d in sorted(self.devices, key=lambda x: x['slave_id']):
                    sim = d['sim']
                    try:
                        lock = getattr(sim, '_current_lock', None)
                        if lock:
                            with lock:
                                cur = dict(sim._current)  # Thread-safe snapshot
                        else:
                            cur = dict(sim._current)  # Snapshot to avoid race
                    except RuntimeError:
                        continue  # dict changed size during iteration
                    sid = d['slave_id']
                    dtype = d['type']
                    name = d['name']

                    if dtype == 'inverter':
                        fc = "FC04" if d['protocol'].lower().startswith('kstar') else "FC03"
                        print(f"\n  [{name.upper()}] Slave ID: {sid} | {fc}")
                        print("-" * 80)
                        status = cur.get('on_off', cur.get('status', 'N/A'))
                        sun = cur.get('sun_factor', 0) * 100
                        pv_kw = cur.get('pv_power_kw', 0)
                        ac_kw = cur.get('ac_power_kw', 0)
                        voltage = cur.get('voltage', 0)
                        print(f"  Status: {status} | Sun: {sun:.0f}%")
                        print(f"  PV: {pv_kw:.2f} kW | AC: {ac_kw:.2f} kW | V: {voltage:.1f} V")

                    elif dtype == 'relay':
                        print(f"\n  [RELAY {name}] Slave ID: {sid} | FC03 (PCC)")
                        print("-" * 80)
                        inv_kw = cur.get('inverter_kw', 0)
                        load_kw = cur.get('load_kw', 0)
                        net_kw = cur.get('net_kw', 0)
                        do_st = cur.get('do_status', 0)
                        rx_kwh = cur.get('received_kwh', 0)
                        tx_kwh = cur.get('sent_kwh', 0)
                        direction = "<<EXPORT" if net_kw < 0 else "IMPORT>>"
                        print(f"  INV: {inv_kw:.1f}kW | Load: {load_kw:.1f}kW | Net: {net_kw:.1f}kW [{direction}]")
                        print(f"  DO: 0x{do_st:04X} | +WH: {rx_kwh:.1f}kWh | -WH: {tx_kwh:.1f}kWh")

                    elif dtype == 'weather':
                        print(f"\n  [WEATHER {name}] Slave ID: {sid} | FC03")
                        print("-" * 80)
                        rad = cur.get('radiation', 0)
                        temp = cur.get('air_temp', 0)
                        hum = cur.get('humidity', 0)
                        print(f"  Radiation: {rad:.0f} W/m2 | Temp: {temp:.1f}C | Humidity: {hum:.1f}%")

                    # Show Modbus log if available
                    if hasattr(sim, 'store') and hasattr(sim.store, 'store'):
                        for btype in ('h', 'i'):
                            block = sim.store.store.get(btype)
                            if block and hasattr(block, 'log_queue') and block.log_queue:
                                recent = list(block.log_queue)[-2:]
                                for log in recent:
                                    print(f"    {log}")

                print("\n" + "=" * 80)
                print("  Press Ctrl+C to stop")

                time.sleep(2)

            except Exception as e:
                self.logger.error(f"Display error: {e}")
                time.sleep(2)
    
    def start(self):
        """Start the multi-device simulator"""
        print("\n  Starting Modbus server...")
        print()

        self.running = True
        for d in self.devices:
            d['sim'].running = True

        self._update_thread = threading.Thread(target=self._update_loop, daemon=True, name="update")
        self._update_thread.start()

        self._display_thread = threading.Thread(target=self._display_status, daemon=True, name="display")
        self._display_thread.start()

        try:
            identity = ModbusDeviceIdentification()
            identity.VendorName = 'Solarize'
            identity.ProductCode = 'EQUIP-SIM'
            identity.VendorUrl = 'http://solarize.co.kr'
            identity.ProductName = 'Equipment Simulator'
            identity.ModelName = 'Multi-Device'
            identity.MajorMinorRevision = self.VERSION

            StartSerialServer(
                context=self.context,
                identity=identity,
                port=self.port,
                baudrate=self.baudrate,
                bytesize=8,
                parity='N',
                stopbits=1,
                timeout=1
            )
        except KeyboardInterrupt:
            print("\n\nShutting down...")
        except Exception as e:
            import traceback
            print(f"\nError: {e}")
            traceback.print_exc()
        finally:
            self.running = False
            for d in self.devices:
                d['sim'].running = False
            # Wait for threads to finish
            if hasattr(self, '_update_thread') and self._update_thread.is_alive():
                self._update_thread.join(timeout=5)
            if hasattr(self, '_display_thread') and self._display_thread.is_alive():
                self._display_thread.join(timeout=5)
            if (hasattr(self, '_update_thread') and self._update_thread.is_alive()) or \
               (hasattr(self, '_display_thread') and self._display_thread.is_alive()):
                self.logger.warning("Threads did not exit gracefully")


def _generate_rtu_config(devices, inv_models):
    """Generate matching RTU rs485_ch1.ini from simulator device config"""
    import configparser

    # Load inverter features from device_models.ini
    ini_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            'config', 'device_models.ini')
    features = {}
    if os.path.isfile(ini_path):
        cfg = configparser.ConfigParser()
        cfg.read(ini_path, encoding='utf-8')
        if cfg.has_section('inverter_features'):
            for mid, vals in cfg.items('inverter_features'):
                parts = [v.strip() for v in vals.split(',')]
                features[int(mid)] = {
                    'iv_scan': parts[0].lower() == 'true' if len(parts) > 0 else False,
                    'kdn': parts[1].lower() == 'true' if len(parts) > 1 else False,
                }

    # Build model lookup: protocol -> model info
    model_by_proto = {}
    for m in inv_models:
        model_by_proto[m['protocol']] = m

    lines = []
    lines.append("# ============================================================================")
    lines.append("# RS485 Channel 1 Device Configuration")
    lines.append("# Auto-generated by Equipment Simulator")
    lines.append(f"# Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("# ============================================================================")
    lines.append("")

    for i, dev in enumerate(devices):
        sid = dev['slave_id']
        dtype = dev['type']
        name = dev['name']
        proto = dev.get('protocol', '')
        section = f"device_{i+1}"

        lines.append(f"# ----------------------------------------------------------------------------")

        if dtype == 'inverter':
            model_info = model_by_proto.get(proto, {})
            model_id = model_info.get('id', 1)
            feat = features.get(model_id, {'iv_scan': False, 'kdn': False})

            # Determine mppt/string defaults by protocol
            if proto.lower().startswith('kstar'):
                mppt, string = 3, 9
            elif proto.lower().startswith('huawei'):
                mppt, string = 4, 8
            else:
                mppt, string = 4, 8

            iv_scan_str = 'true' if feat['iv_scan'] else 'false'
            control = 'DER_AVM' if feat['kdn'] else 'NONE'

            lines.append(f"# Inverter {i+1} - {name} (protocol: {proto})")
            lines.append(f"# ----------------------------------------------------------------------------")
            lines.append(f"[{section}]")
            lines.append(f"slave_id = {sid}")
            lines.append(f"installed = YES")
            lines.append(f"device_number = {i+1}")
            lines.append(f"device_type = 1")
            lines.append(f"protocol = {proto}")
            lines.append(f"model = {model_id}")
            lines.append(f"mppt_count = {mppt}")
            lines.append(f"string_count = {string}")
            lines.append(f"iv_scan = {iv_scan_str}")
            lines.append(f"iv_scan_data_points = 64")
            lines.append(f"control = {control}")
            lines.append(f"zee_control = false")
            lines.append(f"simulation = false")

        elif dtype == 'relay':
            proto = dev.get('protocol', 'relay')
            mid = dev.get('model_id', 1)
            lines.append(f"# Protection Relay {i+1} - {name} (protocol: {proto})")
            lines.append(f"# ----------------------------------------------------------------------------")
            lines.append(f"[{section}]")
            lines.append(f"slave_id = {sid}")
            lines.append(f"installed = YES")
            lines.append(f"device_number = {i+1}")
            lines.append(f"device_type = 4")
            lines.append(f"protocol = {proto}")
            lines.append(f"model = {mid}")
            lines.append(f"iv_scan = false")
            lines.append(f"control = NONE")
            lines.append(f"zee_control = false")
            lines.append(f"simulation = false")

        elif dtype == 'weather':
            proto = dev.get('protocol', 'weather')
            mid = dev.get('model_id', 1)
            lines.append(f"# Weather Station {i+1} - {name} (protocol: {proto})")
            lines.append(f"# ----------------------------------------------------------------------------")
            lines.append(f"[{section}]")
            lines.append(f"slave_id = {sid}")
            lines.append(f"installed = YES")
            lines.append(f"device_number = {i+1}")
            lines.append(f"device_type = 5")
            lines.append(f"protocol = {proto}")
            lines.append(f"model = {mid}")
            lines.append(f"simulation = false")

        lines.append("")

    # Save to config directory as rs485_ch1.ini (dashboard Config -> Apply & Restart)
    config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config')
    out_path = os.path.join(config_dir, 'rs485_ch1.ini')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"  RTU config saved: {out_path}")
    print("  -> Dashboard Config tab -> Apply & Restart RTU")


def _load_device_models_ini():
    """Load all device models from device_models.ini"""
    import configparser
    ini_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            'config', 'device_models.ini')
    result = {'inverter': [], 'relay': [], 'weather': []}
    if os.path.isfile(ini_path):
        cfg = configparser.ConfigParser()
        cfg.read(ini_path, encoding='utf-8')
        # Inverters
        names = dict(cfg.items('inverter_models')) if cfg.has_section('inverter_models') else {}
        protocols = dict(cfg.items('inverter_protocols')) if cfg.has_section('inverter_protocols') else {}
        for mid, name in sorted(names.items(), key=lambda x: int(x[0])):
            proto = protocols.get(mid, 'unknown')
            result['inverter'].append({'id': int(mid), 'name': name, 'protocol': proto})
        # Relays
        names = dict(cfg.items('relay_models')) if cfg.has_section('relay_models') else {}
        protocols = dict(cfg.items('relay_protocols')) if cfg.has_section('relay_protocols') else {}
        for mid, name in sorted(names.items(), key=lambda x: int(x[0])):
            proto = protocols.get(mid, 'relay')
            result['relay'].append({'id': int(mid), 'name': name, 'protocol': proto})
        # Weather
        names = dict(cfg.items('weather_models')) if cfg.has_section('weather_models') else {}
        protocols = dict(cfg.items('weather_protocols')) if cfg.has_section('weather_protocols') else {}
        for mid, name in sorted(names.items(), key=lambda x: int(x[0])):
            proto = protocols.get(mid, 'weather')
            result['weather'].append({'id': int(mid), 'name': name, 'protocol': proto})
    # Defaults
    if not result['inverter']:
        result['inverter'] = [
            {'id': 1, 'name': 'Solarize Verterking', 'protocol': 'solarize'},
            {'id': 2, 'name': 'Huawei SUN2000', 'protocol': 'huawei'},
            {'id': 3, 'name': 'Kstar KSG-60KT', 'protocol': 'kstar'},
        ]
    if not result['relay']:
        result['relay'] = [{'id': 1, 'name': 'KDU-300', 'protocol': 'relay'}]
    if not result['weather']:
        result['weather'] = [{'id': 1, 'name': 'SEM5046', 'protocol': 'weather'}]
    return result


def _load_inverter_models():
    """Load inverter models (backward compatible)"""
    return _load_device_models_ini()['inverter']


def _calc_isc_cap(voc, nominal_power_w, total_strings):
    """Isc 상한 계산: 전 스트링 Pmpp 합산 ≤ 인버터 용량.

    정규화 Pmpp(Isc=1)를 단일 다이오드 모델로 계산하여 역산.
    """
    ns = max(1, voc / 50.0)
    n_vt_ns = 1.3 * 0.026 * ns
    # 정규화 Pmpp (Isc=1일 때 최대 파워)
    pmpp_norm = 0
    for i in range(200):
        v = 200 + (voc - 200) * i / 199
        cur = 1.0 - math.exp((v - voc) / n_vt_ns)
        cur = max(0.0, min(1.0, cur))
        p = v * cur
        if p > pmpp_norm:
            pmpp_norm = p
    if pmpp_norm <= 0:
        return 10.0  # fallback
    return nominal_power_w / (total_strings * pmpp_norm)


def _create_inverter_by_protocol(protocol, logger, env=None):
    """Create inverter simulator — 모든 프로토콜을 GenericInverterSimulator로 통합."""
    return GenericInverterSimulator(protocol, logger, env=env)


def _load_config_from_ini():
    """Read rs485_ch1.ini + rtu_config.ini -> device list for simulator.

    New default config source (replaces simulator_config.json).
    rs485_ch1.ini is the single source of truth for device configuration,
    shared by both RTU and simulator.
    """
    import configparser
    config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config')
    ini_path = os.path.join(config_dir, 'rs485_ch1.ini')

    if not os.path.isfile(ini_path):
        raise FileNotFoundError(f"rs485_ch1.ini not found: {ini_path}")

    cfg = configparser.ConfigParser()
    cfg.read(ini_path, encoding='utf-8')

    # Load model names from device_models.ini for display
    all_models = _load_device_models_ini()
    model_names = {}
    for dtype_key in ('inverter', 'relay', 'weather'):
        for m in all_models[dtype_key]:
            model_names[(dtype_key, m['protocol'])] = m['name']

    # device_type mapping: 1=inverter, 4=relay, 5=weather
    dtype_map = {1: 'inverter', 4: 'relay', 5: 'weather'}

    devices = []
    for section in sorted(cfg.sections()):
        if not section.startswith('device_'):
            continue
        installed = cfg.get(section, 'installed', fallback='NO').strip().upper()
        if installed != 'YES':
            continue

        sid = cfg.getint(section, 'slave_id')
        dtype_id = cfg.getint(section, 'device_type', fallback=1)
        protocol = cfg.get(section, 'protocol', fallback='solarize').strip()
        model_id = cfg.getint(section, 'model', fallback=1)
        name = cfg.get(section, 'name', fallback='').strip()

        dtype = dtype_map.get(dtype_id, 'inverter')

        # Auto-generate name from device_models.ini or protocol
        if not name:
            name = model_names.get((dtype, protocol), f'{protocol.title()} #{sid}')

        devices.append({
            'slave_id': sid,
            'type': dtype,
            'name': name,
            'protocol': protocol,
            'model_id': model_id,
        })

    # COM 포트: PC에서 USB-RS485 자동 감지 (rtu_config.ini는 CM4용이라 무시)
    port = 'COM10'
    baudrate = 9600
    try:
        import serial.tools.list_ports
        usb_ports = [p.device for p in serial.tools.list_ports.comports()
                     if 'USB' in (p.description or '').upper()]
        if usb_ports:
            port = usb_ports[0]
    except Exception:
        pass

    return {'port': port, 'baudrate': baudrate, 'devices': devices}


def _interactive_setup():
    """Interactive device configuration (fallback when rs485_ch1.ini missing)"""
    print("=" * 70)
    print("  Equipment Simulator v1.4.0")
    print("  Modbus RTU Slave Simulator (Multi-Device)")
    print("=" * 70)
    print()

    # COM port
    port_input = input("  COM Port [COM10]: ").strip()
    port = port_input if port_input else "COM10"

    baud_input = input("  Baudrate [9600]: ").strip()
    baudrate = int(baud_input) if baud_input else 9600

    # Device count
    print()
    count_input = input("  Number of devices [5]: ").strip()
    device_count = int(count_input) if count_input else 5

    all_models = _load_device_models_ini()
    inv_models = all_models['inverter']
    relay_models = all_models['relay']
    weather_models = all_models['weather']

    devices = []
    for i in range(device_count):
        print(f"\n  --- Device {i+1} (Slave ID {i+1}) ---")
        sid_input = input(f"    Slave ID [{i+1}]: ").strip()
        slave_id = int(sid_input) if sid_input else (i + 1)

        print("    Device type:")
        print("      [1] Inverter")
        print("      [2] Protection Relay")
        print("      [3] Weather Station")
        type_input = input("    Select [1]: ").strip()
        dev_type = int(type_input) if type_input else 1

        if dev_type == 1:
            print("    Inverter model:")
            for j, m in enumerate(inv_models):
                print(f"      [{j+1}] {m['name']} (protocol: {m['protocol']})")
            proto_input = input(f"    Select [1]: ").strip()
            proto_idx = int(proto_input) - 1 if proto_input else 0
            proto_idx = max(0, min(proto_idx, len(inv_models) - 1))
            selected = inv_models[proto_idx]
            devices.append({
                'slave_id': slave_id,
                'type': 'inverter',
                'name': selected['name'],
                'protocol': selected['protocol'],
                'model_id': selected['id'],
            })
        elif dev_type == 2:
            print("    Relay model:")
            for j, m in enumerate(relay_models):
                print(f"      [{j+1}] {m['name']} (protocol: {m['protocol']})")
            r_input = input(f"    Select [1]: ").strip()
            r_idx = int(r_input) - 1 if r_input else 0
            r_idx = max(0, min(r_idx, len(relay_models) - 1))
            selected = relay_models[r_idx]
            devices.append({
                'slave_id': slave_id,
                'type': 'relay',
                'name': selected['name'],
                'protocol': selected['protocol'],
                'model_id': selected['id'],
            })
        elif dev_type == 3:
            print("    Weather station model:")
            for j, m in enumerate(weather_models):
                print(f"      [{j+1}] {m['name']} (protocol: {m['protocol']})")
            w_input = input(f"    Select [1]: ").strip()
            w_idx = int(w_input) - 1 if w_input else 0
            w_idx = max(0, min(w_idx, len(weather_models) - 1))
            selected = weather_models[w_idx]
            devices.append({
                'slave_id': slave_id,
                'type': 'weather',
                'name': selected['name'],
                'protocol': selected['protocol'],
                'model_id': selected['id'],
            })

    # Summary
    print("\n" + "=" * 70)
    print("  Device Configuration:")
    for d in devices:
        fc = "FC04" if d['protocol'].lower().startswith('kstar') else "FC03"
        if d['type'] != 'inverter':
            fc = "FC03"
        print(f"    [Slave {d['slave_id']}] {d['type'].title():10s} - {d['name']} ({fc})")
    print("=" * 70)

    # Save config
    config = {
        'port': port,
        'baudrate': baudrate,
        'devices': devices,
    }
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'simulator_config.json')
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    print(f"  Config saved: {config_path}")

    return config


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )

    parser = argparse.ArgumentParser(
        description='Equipment Simulator v1.4.0'
    )
    parser.add_argument('--port', type=str, default=None,
                        help='Serial port (overrides config)')
    parser.add_argument('--baudrate', type=int, default=None,
                        help='Baudrate (overrides config)')
    parser.add_argument('--config', type=str, default=None,
                        help='Load config JSON (backward compat, overrides INI)')
    # Legacy args for backward compatibility
    parser.add_argument('--inverter-id', type=int, default=None)
    parser.add_argument('--relay-id', type=int, default=None)
    parser.add_argument('--kstar-id', type=int, default=None)
    parser.add_argument('--huawei-id', type=int, default=None)

    args = parser.parse_args()

    config = None

    # Legacy mode: if old-style args provided
    if args.inverter_id is not None or args.relay_id is not None:
        inv_id = args.inverter_id or 1
        relay_id = args.relay_id or 2
        kstar_id = args.kstar_id or 4
        huawei_id = args.huawei_id or 5
        config = {
            'port': args.port or 'COM10',
            'baudrate': args.baudrate or 9600,
            'devices': [
                {'slave_id': inv_id, 'type': 'inverter', 'name': 'Solarize Verterking', 'protocol': 'solarize'},
                {'slave_id': relay_id, 'type': 'relay', 'name': 'KDU-300', 'protocol': 'relay', 'model_id': 1},
                {'slave_id': 3, 'type': 'weather', 'name': 'SEM5046', 'protocol': 'weather', 'model_id': 1},
                {'slave_id': kstar_id, 'type': 'inverter', 'name': 'Kstar KSG-60KT', 'protocol': 'kstar'},
                {'slave_id': huawei_id, 'type': 'inverter', 'name': 'Huawei SUN2000', 'protocol': 'huawei'},
            ]
        }
    elif args.config:
        # Priority 1: CLI --config (JSON file, backward compat)
        try:
            with open(args.config, encoding='utf-8') as f:
                config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"  [ERROR] Config file error: {e}")
            sys.exit(1)
    else:
        # Priority 2: Read from rs485_ch1.ini (new default)
        try:
            config = _load_config_from_ini()
            if config.get('devices'):
                # 시뮬레이션 가능한 모델 리스트 (common/ 스캔)
                common_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common')
                avail_protos = []
                for f in sorted(os.listdir(common_dir)):
                    if f.endswith('_registers.py') and not f.startswith('REF_'):
                        proto = f.replace('_registers.py', '').split('_')[0].lower()
                        avail_protos.append((proto, f.replace('_registers.py', '')))

                print(f"\n  Available models ({len(avail_protos)}):")
                active_protos = {d['protocol'].lower() for d in config['devices']}
                for proto, fname in avail_protos:
                    active = '*' if proto in active_protos else ' '
                    print(f"    [{active}] {fname}")

                print(f"\n  Active devices ({len(config['devices'])} from rs485_ch1.ini):")
                for d in config['devices']:
                    fc = "FC04" if d['type'] == 'inverter' and d['protocol'].lower().startswith('kstar') else "FC03"
                    print(f"    [Slave {d['slave_id']:2d}] {d['type']:8s} {d['protocol']:12s} {d['name']} ({fc})")
                print()
            else:
                config = None
        except Exception as e:
            print(f"  rs485_ch1.ini load failed: {e}")
            config = None

        # Priority 3: Fallback to simulator_config.json
        if not config or not config.get('devices'):
            auto_cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'simulator_config.json')
            if os.path.isfile(auto_cfg):
                print(f"  Fallback: loading {auto_cfg}")
                try:
                    with open(auto_cfg, encoding='utf-8') as f:
                        config = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError) as e:
                    print(f"  [WARNING] Config load error: {e}")
                    config = None

    # Priority 4: Interactive setup
    if config is None or not config.get('devices'):
        config = _interactive_setup()

    # Override port/baudrate from CLI (with validation)
    if args.port:
        config['port'] = args.port
    if args.baudrate:
        if args.baudrate not in (1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200):
            print(f"  [WARNING] Non-standard baudrate: {args.baudrate}")
        config['baudrate'] = args.baudrate

    # Ensure port/baudrate defaults
    if 'port' not in config:
        config['port'] = 'COM10'
    if 'baudrate' not in config:
        config['baudrate'] = 9600

    # COM 포트 확인/변경 (CLI --port 미지정 시)
    if not args.port:
        port_input = input(f"  COM Port [{config['port']}]: ").strip()
        if port_input:
            config['port'] = port_input

    simulator = EquipmentSimulator(config)
    simulator.start()


if __name__ == '__main__':
    main()
