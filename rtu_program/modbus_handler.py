#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modbus Handler for RTU - Multi-Platform Support
Reads data from Solarize Inverter, KDU-300 Relay, and SEM5046 Weather Station
Version: 3.0.0

Supported Hardware:
  - CM4-ETH-RS485-BASE-B (native UART via pyserial) [NEW in V3.0.0]
  - Waveshare 2-CH RS485 HAT (SPI/SC16IS752) [legacy support]
  - Standard serial port via pymodbus [PC testing]
  - Simulation mode [no hardware]

Changes in 3.0.0:
- Added CM4-ETH-RS485-BASE-B native UART support (ModbusHandlerCM4)
- Added cm4_serial driver package (pyserial-based RS485Channel)
- MultiDeviceHandler now supports use_cm4 parameter
- Platform auto-detection for CM4

Changes in 2.0.1:
- Fixed missing datetime import for weather simulation mode

Changes in 2.0.0:
- Added SEM5046 weather station support
- Added read_weather_data() method for all handlers
- Weather data: radiation, temperature, humidity, wind, etc.

Changes in 1.9.0:
- Added 1 second delay after RS485 HAT initialization
- Fixes initial communication failures on Pi Zero after reboot
- Pi Zero requires more stabilization time than Pi 5

Changes in 1.8.5:
- Fixed OFF command status: STANDBY(0x01) -> SHUTDOWN(0x09)
- Server OFF command now sets inverter to Shutdown mode per protocol

Changes in 1.8.4:
- Fixed AC current scaling: 0.01A register -> 0.1A for H01 protocol
- Previous: /100 (wrong, gave 0.1A when should be ~1.5A)
- Now: /10 (correct, register 150 -> H01 value 15 -> 1.5A)

Changes in 1.8.3:
- Added inter-register delay (100ms) in read_model_info() and read_device_info()
- Prevents CRC errors from rapid consecutive register reads
- Improves communication stability with slow-responding inverters

Changes in 1.8.2:
- Fixed RS485.RS485() import error - changed to RS485()
- HAT connection now works correctly

Changes in 1.8.1:
- Added slave_id parameter to ModbusHandlerSimulation
- Fixed read_device_info() AttributeError in simulation mode
- Note: DER-AVM uses CH2 for Modbus Slave communication (separate module)

Changes in 1.8.0:
- Added read_device_info() for complete inverter information
- Reads all 12 device info registers per Solarize protocol
- Includes model, serial, firmware versions, nominal values, phase info
- Added to HAT, Serial, and Simulation handlers

Changes in 1.7.0:
- Added comprehensive Modbus communication diagnostics
- Added get_diagnostic_report() for field troubleshooting
- Added communication statistics logging
- Integrated with ModbusMaster v1.3.0 exception handling
- Added periodic stats logging (every 100 transactions)

Changes in 1.6.1:
- Added per-device simulation mode support
- add_inverter() and add_relay() now accept simulation parameter
- Each device can be individually configured for simulation or real Modbus

Changes in 1.6.0:
- Added KDU-300 protection relay Modbus reading support
- Added read_relay_data() method for HAT, Serial, and Simulation handlers
- Added relay_registers.py for KDU-300 register map
- Uses Input Registers (Function Code 04) for relay data

Changes in 1.5.9:
- Added get_iv_scan_data() to ModbusHandlerHAT for reading IV curve data
- Reads voltage (0x8000+) and current registers per string mapping

Changes in 1.5.8:
- Added iv_scan_status to read_control_status() for IV Scan polling
- Reads 0x600D register: 0=Idle, 1=Running, 2=Finished

Changes in 1.5.7:
- Added Modbus WRITE logging for IV Scan (0x600D) and all control registers
- Fixed register address comments (IV Scan: 0x600D, not 0x0840)

Changes in 1.5.6:
- Enhanced ModbusHandlerSimulation: control values affect output data
- CTRL_INV_CONTROL_INIT(14) sets PF=1.0, Reactive=0%, Active=100%
- read_inverter_data/read_monitor_data reflect control value changes
- Active/Reactive power calculated as % of nominal rating

Changes in 1.5.5:
- Fixed HAT import: catch FileNotFoundError/OSError when SPI not available

Changes in 1.5.4:
- Added serial number reading in read_model_info() (0x1A10, 8 regs)

Changes in 1.5.3:
- Fixed read_model_info(): removed STRING_COUNT register (managed via config)

Changes in 1.5.2:
- Fixed ModbusHandlerSerial: complete rewrite to match HAT version logic

Changes in 1.5.1:
- Fixed STRING data reading: read 16 registers (8 V/I pairs), extract currents only

Changes in 1.5.0:
- Fixed voltage/current scaling: register value (0.1V, 0.01A) -> protocol unit (V, A)
"""

import time
import logging
import sys
import os
import math
import random
import threading
from datetime import datetime

# Add library path
libdir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'lib')
sys.path.append(libdir)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import importlib as _importlib
from common.Solarize_PV_50kw_registers import RegisterMap, InverterMode, SCALE, registers_to_u32

# --- Solarize_PV_50kw_registers 모듈 참조 (fallback 용) ---
import common.Solarize_PV_50kw_registers as _default_reg_module
# Kstar/Huawei 전용 import 제거됨 — 범용 핸들러가 동적 로딩으로 처리
from common.REF_relay_registers import KDU300RegisterMap, registers_to_float, H01_RELAY_FIELD_MAP
from common.REF_weather_registers import (
    SEM5046RegisterMap,
    raw_to_air_temp, raw_to_humidity, raw_to_pressure,
    raw_to_wind_speed, raw_to_wind_direction, raw_to_module_temp,
    raw_to_accum_radiation, H01_WEATHER_BODY_SIZE
)
from common.protocol_constants import (
    INV_STATUS_ON_GRID, INV_STATUS_STANDBY, INV_STATUS_FAULT,
    CTRL_INV_ON_OFF, CTRL_INV_ACTIVE_POWER, CTRL_INV_POWER_FACTOR,
    CTRL_INV_REACTIVE_POWER, CTRL_INV_IV_SCAN, CTRL_INV_CONTROL_INIT
)

# Check for CM4 native serial (CM4-ETH-RS485-BASE-B)
CM4_SERIAL_AVAILABLE = False
try:
    cm4dir = os.path.join(libdir, 'cm4_serial')
    sys.path.append(cm4dir)
    from cm4_serial.rs485_channel_serial import RS485ChannelSerial
    from cm4_serial.config import get_serial_port, is_cm4
    # ModbusMaster is in the lib directory (hardware-agnostic)
    from modbus_master import ModbusMaster as CM4ModbusMaster
    CM4_SERIAL_AVAILABLE = True
except (ImportError, FileNotFoundError, OSError, Exception):
    CM4_SERIAL_AVAILABLE = False

# Check for Waveshare HAT (Raspberry Pi only)
try:
    hatdir = os.path.join(libdir, 'waveshare_2_CH_RS485_HAT')
    sys.path.append(hatdir)
    from RS485 import RS485
    from rs485_channel import RS485Channel
    from modbus_master import ModbusMaster
    import RPi.GPIO as GPIO
    HAT_AVAILABLE = True
except (ImportError, FileNotFoundError, OSError, Exception) as e:
    HAT_AVAILABLE = False

# Check for pymodbus (PC or standard serial)
try:
    from pymodbus.client import ModbusSerialClient, ModbusTcpClient
    PYMODBUS_AVAILABLE = True
except ImportError:
    PYMODBUS_AVAILABLE = False


# =========================================================================
# Dynamic Register Module Loading
# =========================================================================

def load_register_module(protocol_name: str):
    """protocol 이름으로 common/{protocol}_registers.py 동적 import.

    1차: common.{protocol_name}_registers 정확히 시도
    2차: common/ 디렉토리에서 {manufacturer}_*_registers.py glob fallback
         (예: 'solarize' → 'Solarize_PV_50kw_registers.py')

    Args:
        protocol_name: config의 protocol 값 (예: 'solarize', 'Solarize_PV_50kw')

    Returns:
        Loaded register module (has RegisterMap, SCALE, etc.)
    """
    log = logging.getLogger(__name__)

    # 1차: 정확한 이름
    module_name = f"common.{protocol_name}_registers"
    try:
        mod = _importlib.import_module(module_name)
        log.info(f"Loaded register module: {module_name}")
        return mod
    except ImportError:
        pass

    # 2차: 대소문자 무관 제조사 prefix glob fallback
    import glob as _glob, os as _os
    common_dir = _os.path.join(_os.path.dirname(__file__), '..', 'common')
    prefix = protocol_name.split('_')[0].lower()  # 'solarize', 'ekos', ...
    candidates = sorted(
        _glob.glob(_os.path.join(common_dir, f'*_registers.py'))
    )
    for fpath in candidates:
        fname = _os.path.basename(fpath)
        if fname.lower().startswith(prefix) and not fname.startswith('REF_'):
            mod_name = f"common.{fname[:-3]}"  # strip .py
            try:
                mod = _importlib.import_module(mod_name)
                log.info(f"Loaded register module (fallback): {mod_name} for protocol '{protocol_name}'")
                return mod
            except ImportError:
                continue

    log.warning(f"Register module for '{protocol_name}' not found, fallback to solarize")
    return _default_reg_module


def _normalize_scale(raw_scale: dict) -> dict:
    """Model Maker 생성 SCALE(레지스터명 키) → 일반 키 형식으로 변환.

    solarize_registers.py의 SCALE은 일반명('voltage', 'power' 등)을 사용하지만,
    Model Maker가 자동 생성한 SCALE은 레지스터명('PV_VOLTAGE', 'AC_POWER' 등)을 사용.
    두 형식 모두 호환되도록 일반명 키를 추가한다.
    """
    # 이미 일반 키 형식이면 그대로 반환
    if 'voltage' in raw_scale or 'power' in raw_scale:
        return raw_scale
    # 레지스터명 → 일반명 매핑 (첫 번째 매칭 사용)
    normalized = dict(raw_scale)
    _GENERIC_MAP = {
        'voltage':      ['PV_VOLTAGE', 'R_PHASE_VOLTAGE'],
        'current':      ['PV_CURRENT', 'R_PHASE_CURRENT'],
        'power':        ['AC_POWER', 'PV_POWER', 'ACTIVE_POWER_SETPOINT'],
        'frequency':    ['FREQUENCY'],
        'power_factor': ['POWER_FACTOR'],
    }
    # 기본값: SCALE에 매칭 키가 없으면 solarize 기본 스케일 사용
    _DEFAULTS = {
        'voltage': 0.1, 'current': 0.01, 'power': 0.1,
        'frequency': 0.01, 'power_factor': 0.001,
    }
    for generic_key, reg_keys in _GENERIC_MAP.items():
        found = False
        for rk in reg_keys:
            if rk in raw_scale:
                normalized[generic_key] = raw_scale[rk]
                found = True
                break
        if not found:
            normalized[generic_key] = _DEFAULTS.get(generic_key, 1.0)
    return normalized


# ── H01 Converter Functions ──────────────────────────────────────────────
# raw register value * SCALE → H01 정수 변환
# float32 타입은 이미 물리값이므로 _H01_FLOAT_CONVERTERS 사용
_H01_CONVERTERS = {
    'voltage_to_V':      lambda raw, sc: int(raw * sc.get('voltage', 0.1)),
    'current_to_01A':    lambda raw, sc: int(raw * sc.get('current', 0.01) * 10),
    'frequency_to_01Hz': lambda raw, sc: int(raw * sc.get('frequency', 0.01) * 10),
    'power_to_W':        lambda raw, sc: int(raw * sc.get('power', 0.1)),
    'pf_raw':            lambda raw, sc: int(raw),
    'energy_kwh_to_Wh':  lambda raw, sc: int(raw * 1000),
    'raw':               lambda raw, sc: int(raw),
}
_H01_FLOAT_CONVERTERS = {
    'voltage_to_V':      lambda v: int(v),
    'current_to_01A':    lambda v: int(v * 10),
    'frequency_to_01Hz': lambda v: int(v * 10),
    'power_to_W':        lambda v: int(v),
    'pf_raw':            lambda v: int(v * 1000),
    'energy_kwh_to_Wh':  lambda v: int(v),
    'raw':               lambda v: int(v),
}
_H01_DEFAULTS = {
    'frequency': 600, 'power_factor': 1000,
    'alarm1': 0, 'alarm2': 0, 'alarm3': 0,
}


def _init_reg_attrs(handler, reg_module):
    """핸들러 인스턴스에 레지스터 모듈 속성을 바인딩하는 공통 헬퍼.

    Args:
        handler: ModbusHandler* 인스턴스
        reg_module: 로드된 레지스터 모듈 (None이면 solarize_registers 사용)
    """
    mod = reg_module if reg_module is not None else _default_reg_module
    handler.reg_module = mod
    handler.RegMap = getattr(mod, 'RegisterMap', RegisterMap)
    handler.scale = _normalize_scale(getattr(mod, 'SCALE', SCALE))
    handler.InvMode = getattr(mod, 'InverterMode', InverterMode)
    handler.reg_to_u32 = getattr(mod, 'registers_to_u32', registers_to_u32)
    handler.reg_to_float32 = getattr(mod, 'registers_to_float32', None)
    handler.reg_data_types = getattr(mod, 'DATA_TYPES', None)
    # Find StatusConverter: scan module for any class ending with 'StatusConverter'.
    # This handles both {Brand}StatusConverter and {Brand}{N}StatusConverter patterns
    # from *2 and *3 register files without hardcoding specific class names.
    handler.status_converter = None
    for attr_name in dir(mod):
        if attr_name.endswith('StatusConverter'):
            handler.status_converter = getattr(mod, attr_name, None)
            if handler.status_converter is not None:
                break

    # DATA_PARSER / READ_BLOCKS — Stage 3 생성 파일에 포함된 경우 바인딩
    handler.data_parser = getattr(mod, 'DATA_PARSER', None)
    handler.read_blocks = getattr(mod, 'READ_BLOCKS', None)
    # use_block_read: READ_BLOCKS + DATA_PARSER가 tuple 형식 (blk_idx, offset, dtype, scale)일 때만
    # Stage3가 생성하는 DATA_PARSER는 문자열 형식 → block read 미사용
    _dp = handler.data_parser
    _dp_is_tuple = (_dp and isinstance(next(iter(_dp.values()), None), (tuple, list)))
    handler.use_block_read = bool(handler.read_blocks and _dp_is_tuple)

    # U32 워드 순서 + H01 필드 매핑 + FC 코드
    handler.u32_word_order = getattr(mod, 'U32_WORD_ORDER', 'LH')
    handler.h01_field_map = getattr(mod, 'H01_FIELD_MAP', None)
    handler.rtu_fc_code = getattr(mod, 'RTU_FC_CODE', 3)

    # H01_FIELD_MAP이 있으면 범용 dynamic read 사용 (모든 프로토콜 통합)
    handler.use_dynamic_read = (
        not handler.use_block_read
        and handler.h01_field_map is not None
    )
    _log = logging.getLogger('modbus_handler')
    _rm = handler.RegMap
    _log.info(f"_init_reg_attrs: slave={getattr(handler, 'slave_id', '?')} "
              f"block={handler.use_block_read} "
              f"h01_map={handler.h01_field_map is not None} "
              f"u32_order={handler.u32_word_order} "
              f"dynamic={handler.use_dynamic_read}")


class ModbusHandlerHAT:
    """Modbus RTU Master using Waveshare 2-CH RS485 HAT"""
    
    VERSION = "1.0.8"
    
    def __init__(self, channel: int = 1, baudrate: int = 9600, slave_id: int = 1,
                 reg_module=None):
        self.channel = channel
        self.baudrate = baudrate
        self.slave_id = slave_id
        self.connected = False
        self.logger = logging.getLogger(__name__)

        self.rs485 = None
        self.rs485_channel = None
        self.master = None

        # Dynamic register module binding
        _init_reg_attrs(self, reg_module)

        # Simulation state
        self._sim_energy = 1000000  # Initial 1000kWh
        self._sim_start = time.time()

        # Communication tracking
        self._read_count = 0
        self._last_stats_log = 0
        self._stats_log_interval = 100  # Log stats every 100 transactions

    def connect(self):
        """Connect to RS485 HAT"""
        if not HAT_AVAILABLE:
            self.logger.error("Waveshare HAT library not available")
            return False
        
        try:
            self.rs485 = RS485()
            
            if self.channel == 1:
                self.rs485.RS485_CH1_begin(self.baudrate)
                self.rs485_channel = RS485Channel(self.rs485, 1)
            else:
                self.rs485.RS485_CH2_begin(self.baudrate)
                self.rs485_channel = RS485Channel(self.rs485, 2)
            
            # Wait for RS485 HAT to stabilize (Pi Zero needs more time)
            import time
            time.sleep(1.0)
            
            self.master = ModbusMaster(self.rs485_channel, self.slave_id, timeout=1.0)
            self.master.set_retry_config(max_retries=3, base_delay=0.1, auto_retry=True)
            self.connected = True
            self.logger.info(f"Connected to RS485 HAT CH{self.channel} @ {self.baudrate}bps (Slave {self.slave_id})")
            return True
            
        except Exception as e:
            self.logger.error(f"HAT connection error: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from HAT"""
        # Log final statistics before disconnect
        if self.master:
            self.logger.info(f"Final Modbus stats: {self._get_stats_summary()}")
        
        self.connected = False
        if HAT_AVAILABLE:
            try:
                GPIO.cleanup()
            except Exception as e:
                self.logger.warning(f"GPIO cleanup error: {e}")
    
    def set_debug(self, enable: bool):
        """Enable/disable Modbus debug output"""
        if self.master:
            self.master.set_debug(enable)
    
    def _check_stats_log(self):
        """Periodically log communication statistics"""
        self._read_count += 1
        if self._read_count - self._last_stats_log >= self._stats_log_interval:
            self._last_stats_log = self._read_count
            if self.master:
                stats = self.master.get_stats()
                self.logger.info(
                    f"Modbus Stats [Slave {self.slave_id}]: "
                    f"Success={stats.successful}/{stats.total_requests} "
                    f"({stats.success_rate:.1f}%), "
                    f"Timeouts={stats.timeouts}, CRC={stats.crc_errors}, "
                    f"AvgTime={stats.avg_response_time:.1f}ms"
                )
    
    def _get_stats_summary(self):
        """Get brief stats summary string"""
        if not self.master:
            return "N/A"
        stats = self.master.get_stats()
        return (
            f"Success={stats.success_rate:.1f}% "
            f"({stats.successful}/{stats.total_requests}), "
            f"Timeouts={stats.timeouts}, CRC={stats.crc_errors}"
        )
    
    def get_diagnostic_report(self):
        """Get comprehensive diagnostic report"""
        if not self.master:
            return "Modbus master not initialized"
        return self.master.get_diagnostic_report()
    
    def get_stats(self):
        """Get communication statistics"""
        if self.master:
            return self.master.get_stats()
        return None

    def get_last_error_str(self) -> str:
        """Get last Modbus error type as string for logging.
        Returns: 'Timeout', 'CRC_Error', 'NoResponse', 'FrameError', 'InvalidResp', or 'Unknown'
        """
        if not self.master:
            return 'NoMaster'
        try:
            stats = self.master.get_stats()
            err = stats.get('error')
            if err:
                err_str = str(err).lower()
                if 'timeout' in err_str:
                    return 'Timeout'
                elif 'crc' in err_str:
                    return 'CRC_Error'
                elif 'no response' in err_str or 'no_response' in err_str:
                    return 'NoResponse'
                elif 'frame' in err_str:
                    return 'FrameError'
                elif 'invalid' in err_str:
                    return 'InvalidResp'
                return err_str[:20]
        except Exception:
            pass
        return 'Unknown'

    def diagnose_connection(self):
        """Run connection diagnostics"""
        if self.master:
            return self.master.diagnose_connection(self.slave_id)
        return {'tests': [], 'recommendations': ['Master not initialized']}
    
    def _read_reg(self, addr, count=1):
        """Read holding registers (FC03), returns list of U16 values or None."""
        return self.master.read_holding_registers(addr, count, self.slave_id)

    def _read_input_reg(self, addr, count=1):
        """Read input registers (FC04), returns list of U16 values or None."""
        return self.master.read_input_registers(addr, count, self.slave_id)

    def _read_block(self, start, count, fc=3):
        """Read a contiguous register block using the specified FC code.

        Returns list of U16 values, or None on failure.
        """
        if fc == 4:
            return self._read_input_reg(start, count)
        return self._read_reg(start, count)

    def _build_reg_cache(self, read_blocks):
        """Read all READ_BLOCKS and return {addr: value} cache.

        Only caches registers that were successfully read.
        """
        cache = {}
        for blk in read_blocks:
            start = blk['start']
            count = blk['count']
            fc = blk.get('fc', 3)
            result = self._read_block(start, count, fc)
            if result:
                for i, v in enumerate(result):
                    cache[start + i] = v
        return cache

    def _get_typed_from_cache(self, field_name, addr, cache):
        """Extract a typed value from the register cache.

        Returns converted Python value (int or float), or None if not cached.
        Handles FLOAT32/U32/S32/S16/U16 via DATA_TYPES.
        U32/S32는 u32_word_order에 따라 워드 순서 결정.
        """
        dtype = (self.reg_data_types or {}).get(field_name, 'u16').lower()
        if dtype == 'float32':
            w0 = cache.get(addr)
            w1 = cache.get(addr + 1)
            if w0 is not None and w1 is not None and self.reg_to_float32:
                if getattr(self, 'u32_word_order', 'LH') == 'HL':
                    return self.reg_to_float32(w1, w0)
                return self.reg_to_float32(w0, w1)
            return None
        elif dtype in ('u32', 's32'):
            w0 = cache.get(addr)
            w1 = cache.get(addr + 1)
            if w0 is not None and w1 is not None:
                if getattr(self, 'u32_word_order', 'LH') == 'HL':
                    lo, hi = w1, w0
                else:
                    lo, hi = w0, w1
                if dtype == 'u32':
                    return self.reg_to_u32(lo, hi)
                else:
                    val = self.reg_to_u32(lo, hi)
                    return val - 0x100000000 if val >= 0x80000000 else val
            return None
        elif dtype == 's16':
            v = cache.get(addr)
            if v is not None:
                return v - 65536 if v > 32767 else v
            return None
        else:  # u16
            return cache.get(addr)

    def _read_typed_value(self, field_name, addr):
        """Read a register value using the correct data type from DATA_TYPES.

        Returns the converted Python value (int or float), or None on failure.
        Uses reg_data_types to determine Float32/U32/S32/S16/U16 handling.
        U32/S32는 u32_word_order에 따라 워드 순서 결정.
        """
        dtype = (self.reg_data_types or {}).get(field_name, 'u16')

        if dtype == 'float32':
            result = self._read_reg(addr, 2)
            if result and len(result) >= 2:
                w0, w1 = result[0], result[1]
                if getattr(self, 'u32_word_order', 'LH') == 'HL':
                    w0, w1 = w1, w0
                return self.reg_to_float32(w0, w1)
            return None
        elif dtype in ('u32', 's32'):
            result = self._read_reg(addr, 2)
            if result and len(result) >= 2:
                w0, w1 = result[0], result[1]
                if getattr(self, 'u32_word_order', 'LH') == 'HL':
                    lo, hi = w1, w0
                else:
                    lo, hi = w0, w1
                if dtype == 'u32':
                    return self.reg_to_u32(lo, hi)
                else:
                    val = self.reg_to_u32(lo, hi)
                    return val - 0x100000000 if val >= 0x80000000 else val
            return None
        elif dtype == 's16':
            result = self._read_reg(addr, 1)
            if result:
                v = result[0]
                return v - 65536 if v > 32767 else v
            return None
        else:  # u16
            result = self._read_reg(addr, 1)
            if result:
                return result[0]
            return None

    def _read_inverter_data_dynamic(self):
        """H01_FIELD_MAP 기반 범용 인버터 데이터 읽기.

        모든 프로토콜 (Solarize, Huawei, Kstar 등)을 RegisterMap + H01_FIELD_MAP +
        DATA_TYPES + SCALE + U32_WORD_ORDER로 통합 처리.

        Strategy:
          1. READ_BLOCKS → batch cache 구축 (한 번의 일괄 읽기)
          2. H01_FIELD_MAP 순회 → 스칼라 필드 변환
          3. MPPT{N}_VOLTAGE/CURRENT 패턴 → mppt 배열
          4. STRING{N}_CURRENT 패턴 → strings 배열
          5. StatusConverter → mode/status 변환

        Returns:
            dict compatible with H01 inverter body, or None on failure.
        """
        self.logger.debug(f"[Dynamic Read] slave={self.slave_id}")
        if not self.connected or not self.master:
            return None

        try:
            rm = self.RegMap
            scale = self.scale
            dtypes = self.reg_data_types or {}
            h01_map = self.h01_field_map or {}

            # ── helper: read one field by address ──────────────────────────
            def _read_field(field, addr, cache):
                if cache is not None:
                    return self._get_typed_from_cache(field, addr, cache)
                return self._read_typed_value(field, addr)

            def _convert(val, conv_key, field_name):
                """raw register value → H01 정수."""
                if val is None:
                    return _H01_DEFAULTS.get(field_name, 0)
                dtype = dtypes.get(field_name, 'u16').lower()
                if dtype == 'float32':
                    fn = _H01_FLOAT_CONVERTERS.get(conv_key, _H01_FLOAT_CONVERTERS['raw'])
                    return fn(val)
                fn = _H01_CONVERTERS.get(conv_key, _H01_CONVERTERS['raw'])
                return fn(val, scale)

            # ── batch read via READ_BLOCKS ──────────────────────────────────
            read_blocks = getattr(self.reg_module, 'READ_BLOCKS', None)
            cache = self._build_reg_cache(read_blocks) if read_blocks else None

            data = {}

            # ── H01_FIELD_MAP 스칼라 필드 처리 ──────────────────────────────
            for h01_key, (reg_attr, conv_key) in h01_map.items():
                addr = getattr(rm, reg_attr, None)
                if addr is None:
                    data[h01_key] = _H01_DEFAULTS.get(h01_key, 0)
                    continue
                val = _read_field(reg_attr, addr, cache)
                data[h01_key] = _convert(val, conv_key, reg_attr)

            # ── Inverter Mode / Status 변환 ─────────────────────────────────
            raw_mode = data.get('mode', 0)
            if self.status_converter and hasattr(self.status_converter, 'to_inverter_mode'):
                mode = self.status_converter.to_inverter_mode(raw_mode)
            else:
                mode = raw_mode
            data['mode'] = mode
            if mode == self.InvMode.ON_GRID:
                data['status'] = INV_STATUS_ON_GRID
            elif mode in (getattr(self.InvMode, 'STANDBY', 1),
                          getattr(self.InvMode, 'INITIAL', 0),
                          getattr(self.InvMode, 'SHUTDOWN', 9)):
                data['status'] = INV_STATUS_STANDBY
            elif mode == getattr(self.InvMode, 'FAULT', 5):
                data['status'] = INV_STATUS_FAULT
            else:
                data['status'] = INV_STATUS_STANDBY

            # ── MPPT Data ───────────────────────────────────────────────────
            mppt_data = []
            for i in range(1, 17):
                v_field = f'MPPT{i}_VOLTAGE'
                c_field = f'MPPT{i}_CURRENT'
                v_addr = getattr(rm, v_field, None)
                if v_addr is None:
                    break
                c_addr = getattr(rm, c_field, None)
                v_val = _read_field(v_field, v_addr, cache)
                c_val = _read_field(c_field, c_addr, cache) if c_addr is not None else None
                v_dtype = dtypes.get(v_field, 'u16').lower()
                c_dtype = dtypes.get(c_field, 'u16').lower()
                mppt_v = (int((v_val or 0) * 10) if v_dtype == 'float32'
                          else int(v_val) if v_val else 0)
                mppt_c = (int((c_val or 0) * 100) if c_dtype == 'float32'
                          else int(c_val) if c_val else 0)
                mppt_data.append({'voltage': mppt_v, 'current': mppt_c})
            data['mppt'] = mppt_data

            if mppt_data:
                connected = [m for m in mppt_data if m['voltage'] >= 1000]  # ≥100V
                data['pv_voltage'] = (int(sum(m['voltage'] for m in connected)
                                         / len(connected) / 10) if connected else 0)
                data['pv_current'] = int(sum(m['current'] for m in mppt_data) / 10)
            else:
                data['pv_voltage'] = 0
                data['pv_current'] = 0

            # ── String Currents ─────────────────────────────────────────────
            strings = []
            for i in range(1, 33):
                s_field = f'STRING{i}_CURRENT'
                s_addr = getattr(rm, s_field, None)
                if s_addr is None:
                    break
                s_val = _read_field(s_field, s_addr, cache)
                s_dtype = dtypes.get(s_field, 'u16').lower()
                strings.append(int((s_val or 0) * 100) if s_dtype == 'float32'
                               else int(s_val) if s_val else 0)
            data['strings'] = strings

            if hasattr(self, '_check_stats_log'):
                self._check_stats_log()
            return data

        except Exception as e:
            self.logger.error(f"Dynamic read error: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None

    def _format_h01_from_raw(self, physical: dict) -> dict:
        """물리값 dict(SI 단위)를 H01 패킷 형식 dict로 변환.

        physical 값은 DATA_PARSER scale 적용 후의 물리 SI 단위:
          전압 V, 전류 A, 전력 W, 주파수 Hz, 에너지 kWh

        반환 dict는 read_inverter_data() 의 표준 출력 형식과 동일.
        """
        data = {}

        # AC Phase 전압 (V → V integer)
        for key, field in [('r_voltage', 'R_PHASE_VOLTAGE'),
                           ('s_voltage', 'S_PHASE_VOLTAGE'),
                           ('t_voltage', 'T_PHASE_VOLTAGE')]:
            val = physical.get(field)
            data[key] = int(val) if val is not None else 0

        # AC Phase 전류 (A → 0.1A integer)
        for key, field in [('r_current', 'R_PHASE_CURRENT'),
                           ('s_current', 'S_PHASE_CURRENT'),
                           ('t_current', 'T_PHASE_CURRENT')]:
            val = physical.get(field)
            data[key] = int(val * 10) if val is not None else 0

        # 주파수 (Hz → 0.1Hz integer)
        val = physical.get('FREQUENCY')
        data['frequency'] = int(val * 10) if val is not None else 600

        # AC 전력 (W integer)
        val = physical.get('AC_POWER')
        data['ac_power'] = int(val) if val is not None else 0

        # PV 전력 (W integer)
        val = physical.get('PV_POWER')
        data['pv_power'] = int(val) if val is not None else 0

        # 역률 (-1.0~1.0 → -1000~1000 integer)
        val = physical.get('POWER_FACTOR')
        if val is not None:
            pf = int(val * 1000)
            data['power_factor'] = max(-1000, min(1000, pf))
        else:
            data['power_factor'] = 1000

        # 누적 발전량 (kWh → Wh integer)
        val = physical.get('TOTAL_ENERGY')
        data['cumulative_energy'] = int(val * 1000) if val is not None else 0

        # MPPT 데이터 (raw register 형식: voltage=0.1V, current=0.01A)
        mppt_data = []
        for i in range(1, 9):
            v = physical.get(f'MPPT{i}_VOLTAGE')
            c = physical.get(f'MPPT{i}_CURRENT')
            if v is None and c is None:
                break
            mppt_data.append({
                'voltage': int(v * 10) if v is not None else 0,   # V → 0.1V raw
                'current': int(c * 100) if c is not None else 0,  # A → 0.01A raw
            })
        data['mppt'] = mppt_data

        # PV 전압/전류 (MPPT 데이터에서 계산)
        if mppt_data:
            connected = [m for m in mppt_data if m['voltage'] >= 1000]  # >= 100V (0.1V)
            data['pv_voltage'] = (int(sum(m['voltage'] for m in connected) /
                                      len(connected) / 10) if connected else 0)
            data['pv_current'] = int(sum(m['current'] for m in mppt_data) / 10)
        else:
            pv_v = physical.get('PV_VOLTAGE')
            pv_c = physical.get('PV_CURRENT')
            data['pv_voltage'] = int(pv_v) if pv_v is not None else 0
            data['pv_current'] = int(pv_c * 10) if pv_c is not None else 0

        # String 전류 (A → 0.01A raw)
        strings = []
        for i in range(1, 17):
            val = physical.get(f'STRING{i}_CURRENT')
            if val is None:
                break
            strings.append(int(val * 100))  # A → 0.01A
        data['strings'] = strings

        # 인버터 모드/상태
        mode_val = physical.get('INVERTER_MODE')
        if mode_val is not None:
            raw_mode = int(mode_val)
            if self.status_converter and hasattr(self.status_converter, 'to_inverter_mode'):
                mode = self.status_converter.to_inverter_mode(raw_mode)
            else:
                mode = raw_mode
            data['mode'] = mode
            if mode == self.InvMode.ON_GRID:
                data['status'] = INV_STATUS_ON_GRID
            elif mode in (self.InvMode.STANDBY, self.InvMode.INITIAL,
                          self.InvMode.SHUTDOWN):
                data['status'] = INV_STATUS_STANDBY
            elif mode == self.InvMode.FAULT:
                data['status'] = INV_STATUS_FAULT
            else:
                data['status'] = INV_STATUS_STANDBY
        else:
            data['mode'] = self.InvMode.ON_GRID
            data['status'] = INV_STATUS_ON_GRID

        # 알람 코드
        data['alarm1'] = int(physical.get('ERROR_CODE1', 0) or 0)
        data['alarm2'] = int(physical.get('ERROR_CODE2', 0) or 0)
        data['alarm3'] = 0

        return data

    def _read_inverter_data_blocks(self):
        """READ_BLOCKS/DATA_PARSER 기반 블록 단위 인버터 데이터 읽기.

        연속된 레지스터를 한 번에 읽어 효율적으로 데이터 수집.
        FLOAT32, U32, S32, U16, S16 모두 지원.
        반환 dict 형식은 read_inverter_data() 표준과 동일.
        """
        import struct as _struct

        if not self.connected or not self.master:
            return None

        try:
            fc = getattr(self, 'fc_code', 3)
            read_blocks = getattr(self, 'read_blocks', []) or []
            data_parser = getattr(self, 'data_parser', {}) or {}

            # 블록 단위 읽기
            block_data: dict = {}
            for i, blk in enumerate(read_blocks):
                if isinstance(blk, dict):
                    start_addr = blk['start']
                    count = blk['count']
                    blk_fc = blk.get('fc', fc)
                else:
                    start_addr, count = blk[0], blk[1]
                    blk_fc = fc
                if blk_fc == 4:
                    result = self.master.read_input_registers(
                        start_addr, count, self.slave_id)
                else:
                    result = self.master.read_holding_registers(
                        start_addr, count, self.slave_id)
                block_data[i] = list(result) if result else [0] * count

            # DATA_PARSER로 물리값 변환
            physical: dict = {}
            for field, (blk_idx, offset, dtype, scale) in data_parser.items():
                block = block_data.get(blk_idx, [])
                if not block or offset >= len(block):
                    continue
                try:
                    dtype_up = dtype.upper()
                    if dtype_up == 'FLOAT32':
                        if offset + 1 >= len(block):
                            continue
                        lo, hi = block[offset], block[offset + 1]
                        packed = _struct.pack('>HH', lo, hi)
                        val = _struct.unpack('>f', packed)[0]
                        if val != val or abs(val) > 1e12:  # NaN/Inf 제거
                            continue
                    elif dtype_up in ('U32', 'S32'):
                        if offset + 1 >= len(block):
                            continue
                        val = self.reg_to_u32(block[offset], block[offset + 1])
                        if dtype_up == 'S32' and val >= 0x80000000:
                            val -= 0x100000000
                    elif dtype_up == 'S16':
                        val = block[offset]
                        if val >= 0x8000:
                            val -= 0x10000
                    else:  # U16 default
                        val = block[offset]
                    physical[field] = val * scale if scale != 1.0 else val
                except Exception:
                    continue

            self._check_stats_log()
            return self._format_h01_from_raw(physical)

        except Exception as e:
            self.logger.error(f"Block read error slave={self.slave_id}: {e}")
            return None

    def read_inverter_data(self):
        """Read inverter data via Modbus"""
        # 1순위: READ_BLOCKS/DATA_PARSER 블록 읽기
        if getattr(self, 'use_block_read', False):
            self.logger.info(f"[INV slave={self.slave_id}] Block read active")
            return self._read_inverter_data_blocks()

        # 2순위: DATA_TYPES 기반 동적 읽기 (비-Solarize)
        dyn = getattr(self, 'use_dynamic_read', False)
        if dyn:
            self.logger.info(f"[INV slave={self.slave_id}] Dynamic read active")
            return self._read_inverter_data_dynamic()

        if not self.connected or not self.master:
            return None

        try:
            data = {}
            
            # Read L1 Phase (0x1001-0x1005)
            result = self.master.read_holding_registers(0x1001, 5, self.slave_id)
            if result:
                data['r_voltage'] = int(result[0] / 10)      # 0.1V register -> V (Scale 1)
                data['r_current'] = int(result[1] / 10)      # 0.01A register -> 0.1A (for H01)
                l1_power = self.reg_to_u32(result[2], result[3]) if len(result) > 3 else 0
                data['frequency'] = int(result[4] / 10) if len(result) > 4 else 600  # 0.01Hz -> 0.1Hz
            else:
                return None
            
            # Read L2 Phase (0x1006-0x100A)
            result = self.master.read_holding_registers(0x1006, 5, self.slave_id)
            if result:
                data['s_voltage'] = int(result[0] / 10)      # 0.1V register -> V (Scale 1)
                data['s_current'] = int(result[1] / 10)      # 0.01A register -> 0.1A (for H01)
            
            # Read L3 Phase (0x100B-0x100F)
            result = self.master.read_holding_registers(0x100B, 5, self.slave_id)
            if result:
                data['t_voltage'] = int(result[0] / 10)      # 0.1V register -> V (Scale 1)
                data['t_current'] = int(result[1] / 10)      # 0.01A register -> 0.1A (for H01)
            
            # Read MPPT data
            mppt_data = []
            
            # MPPT 1-3 (0x1010-0x101B)
            result = self.master.read_holding_registers(0x1010, 12, self.slave_id)
            if result:
                for i in range(3):
                    idx = i * 4
                    mppt_data.append({
                        'voltage': result[idx],       # Raw 0.1V register value
                        'current': result[idx + 1]    # Raw 0.01A register value
                    })
            
            # MPPT 4 (0x103E-0x1041)
            result = self.master.read_holding_registers(0x103E, 4, self.slave_id)
            if result:
                mppt_data.append({
                    'voltage': result[0],       # Raw 0.1V register value
                    'current': result[1]        # Raw 0.01A register value
                })
            
            data['mppt'] = mppt_data
            
            # Calculate PV voltage/current from MPPT data
            # PV voltage = average of connected MPPTs (>= 100V = 1000 raw in 0.1V)
            # PV current = sum of all MPPT currents
            if mppt_data:
                connected = [m for m in mppt_data if m['voltage'] >= 1000]  # >= 100V
                data['pv_voltage'] = int(sum(m['voltage'] for m in connected) / len(connected) / 10) if connected else 0
                data['pv_current'] = int(sum(m['current'] for m in mppt_data) / 10)
            else:
                data['pv_voltage'] = 0
                data['pv_current'] = 0

            # Read String data (0x1050-0x105F) - 8 strings x 2 regs (V,I pairs)
            result = self.master.read_holding_registers(0x1050, 16, self.slave_id)
            if result:
                # Extract only current values (odd indices: 1,3,5,7,9,11,13,15)
                # Raw 0.01A register values, will be converted in protocol_handler
                data['strings'] = [result[i] for i in range(1, 16, 2)]
            else:
                data['strings'] = []
            
            # Read PV Power (0x1048-0x1049)
            result_low = self.master.read_holding_registers(0x1048, 1, self.slave_id)
            result_high = self.master.read_holding_registers(0x1049, 1, self.slave_id)
            if result_low and result_high:
                data['pv_power'] = int(self.reg_to_u32(result_low[0], result_high[0]) * self.scale['power'])
            else:
                data['pv_power'] = 0

            # Read Grid Power (0x1037-0x1038)
            result_low = self.master.read_holding_registers(0x1037, 1, self.slave_id)
            result_high = self.master.read_holding_registers(0x1038, 1, self.slave_id)
            if result_low and result_high:
                data['ac_power'] = int(self.reg_to_u32(result_low[0], result_high[0]) * self.scale['power'])
            else:
                data['ac_power'] = 0
            
            # Read Power Factor (0x103D)
            result = self.master.read_holding_registers(0x103D, 1, self.slave_id)
            if result:
                pf = result[0]
                if pf > 32767:
                    pf = pf - 65536
                data['power_factor'] = pf
            else:
                data['power_factor'] = 1000
            
            # Read Status (0x101C-0x1020)
            result = self.master.read_holding_registers(0x101C, 5, self.slave_id)
            if result:
                mode = result[1]
                data['mode'] = mode  # Store raw mode value for logging
                if mode == self.InvMode.ON_GRID:
                    data['status'] = INV_STATUS_ON_GRID
                elif mode in (self.InvMode.STANDBY, self.InvMode.INITIAL,
                              self.InvMode.SHUTDOWN):
                    data['status'] = INV_STATUS_STANDBY
                elif mode == self.InvMode.FAULT:
                    data['status'] = INV_STATUS_FAULT
                else:
                    data['status'] = INV_STATUS_STANDBY
                data['alarm1'] = result[2] if len(result) > 2 else 0
                data['alarm2'] = result[3] if len(result) > 3 else 0
                data['alarm3'] = result[4] if len(result) > 4 else 0
            else:
                data['mode'] = self.InvMode.ON_GRID
                data['status'] = INV_STATUS_ON_GRID
                data['alarm1'] = data['alarm2'] = data['alarm3'] = 0

            # Read Energy (0x1021-0x1022)
            result_low = self.master.read_holding_registers(0x1021, 1, self.slave_id)
            result_high = self.master.read_holding_registers(0x1022, 1, self.slave_id)
            if result_low and result_high:
                data['cumulative_energy'] = self.reg_to_u32(result_low[0], result_high[0]) * 1000
            else:
                data['cumulative_energy'] = 0
            
            # Periodic stats logging
            self._check_stats_log()
            
            return data
            
        except Exception as e:
            self.logger.error(f"Read error: {e}")
            # Log diagnostic info on error
            if self.master:
                last_tx = self.master.get_last_transaction()
                self.logger.error(f"Last TX: {last_tx['tx']}, RX: {last_tx['rx']}")
            return None
    
    def write_control(self, control_type: int, value: int):
        """Write control to inverter
        
        Supported control types:
        - 12: IV Scan Command (0x600D)
        - 14: Control Init (multiple registers)
        - 15: ON/OFF (0x0834)
        - 16: Active Power % (0x07D3)
        - 17: Power Factor (0x07D0)
        - 18: Reactive Power (0x07D2)
        """
        if not self.connected or not self.master:
            self.logger.warning(f"write_control failed: not connected")
            return False
        
        try:
            reg_map = {
                CTRL_INV_ON_OFF: getattr(self.RegMap, 'INVERTER_ON_OFF', None),
                CTRL_INV_ACTIVE_POWER: getattr(self.RegMap, 'ACTIVE_POWER_PCT', None),
                CTRL_INV_POWER_FACTOR: getattr(self.RegMap, 'POWER_FACTOR_SET', None),
                CTRL_INV_REACTIVE_POWER: getattr(self.RegMap, 'REACTIVE_POWER_SET', None),
                CTRL_INV_IV_SCAN: getattr(self.RegMap, 'IV_SCAN_COMMAND', None),
            }

            if control_type == CTRL_INV_CONTROL_INIT:
                # Control Init: Reset all control values to default
                # ON_OFF: 0=Run(ON), 1=Stop(OFF) in register
                success = True
                if getattr(self.RegMap, 'INVERTER_ON_OFF', None) is not None:
                    success &= self.master.write_single_register(
                        self.RegMap.INVERTER_ON_OFF, 0, self.slave_id)  # 0=Run(ON)
                if getattr(self.RegMap, 'ACTIVE_POWER_PCT', None) is not None:
                    success &= self.master.write_single_register(
                        self.RegMap.ACTIVE_POWER_PCT, 1000, self.slave_id)
                if getattr(self.RegMap, 'POWER_FACTOR_SET', None) is not None:
                    success &= self.master.write_single_register(
                        self.RegMap.POWER_FACTOR_SET, 1000, self.slave_id)
                if getattr(self.RegMap, 'REACTIVE_POWER_SET', None) is not None:
                    success &= self.master.write_single_register(
                        self.RegMap.REACTIVE_POWER_SET, 0, self.slave_id)
                return success

            reg = reg_map.get(control_type)
            if reg is None:
                self.logger.warning(f"Unknown control type: {control_type}")
                return False
            
            self.logger.info(f"Modbus WRITE: Reg=0x{reg:04X} Value={value} (ctrl_type={control_type})")
            result = self.master.write_single_register(reg, value, self.slave_id)
            self.logger.info(f"Modbus WRITE result: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Write error: {e}")
            return False
    
    def read_control_status(self):
        """Read current control register values (for H05 Body Type 13)
        
        Returns dict with:
        - on_off: 0=ON(Run), 1=OFF(Stop)
        - power_factor: float (-1.0 ~ 1.0)
        - operation_mode: int
        - reactive_power_pct: float (%)
        - active_power_pct: float (%)
        """
        if not self.connected or not self.master:
            return None
        
        try:
            status = {}
            
            # ON/OFF: 0=ON(기동), 1=OFF(정지)
            result = self.master.read_holding_registers(self.RegMap.INVERTER_ON_OFF, 1, self.slave_id)
            status['on_off'] = result[0] if result else 0  # 0=ON, 1=OFF (no conversion needed)

            # Power Factor (signed, raw value -1000~1000)
            pf_reg = getattr(self.RegMap, 'POWER_FACTOR_SET',
                             getattr(self.RegMap, 'DER_POWER_FACTOR_SET', 0x07D0))
            result = self.master.read_holding_registers(pf_reg, 1, self.slave_id)
            if result:
                pf = result[0]
                if pf > 32767:
                    pf = pf - 65536
                status['power_factor'] = pf
            else:
                status['power_factor'] = 1000

            # Operation Mode
            mode_reg = getattr(self.RegMap, 'OPERATION_MODE',
                               getattr(self.RegMap, 'DER_ACTION_MODE', 0x07D1))
            result = self.master.read_holding_registers(mode_reg, 1, self.slave_id)
            status['operation_mode'] = result[0] if result else 0

            # Reactive Power % (signed, raw value -1000~1000)
            rp_reg = getattr(self.RegMap, 'REACTIVE_POWER_SET',
                             getattr(self.RegMap, 'DER_REACTIVE_POWER_PCT', 0x07D2))
            result = self.master.read_holding_registers(rp_reg, 1, self.slave_id)
            if result:
                rp = result[0]
                if rp > 32767:
                    rp = rp - 65536
                status['reactive_power_pct'] = rp
            else:
                status['reactive_power_pct'] = 0

            # Active Power % (raw value 0~1000)
            ap_reg = getattr(self.RegMap, 'ACTIVE_POWER_PCT',
                             getattr(self.RegMap, 'DER_ACTIVE_POWER_PCT', 0x07D3))
            result = self.master.read_holding_registers(ap_reg, 1, self.slave_id)
            status['active_power_pct'] = result[0] if result else 1000

            # IV Scan Status (0x600D): 0=Idle, 1=Running, 2=Finished
            iv_scan_reg = getattr(self.RegMap, 'IV_SCAN_STATUS', None)
            if iv_scan_reg is not None:
                result = self.master.read_holding_registers(iv_scan_reg, 1, self.slave_id)
                status['iv_scan_status'] = result[0] if result else 0
            else:
                status['iv_scan_status'] = 0
            
            return status
            
        except Exception as e:
            self.logger.error(f"Read control status error: {e}")
            return None
    
    def get_iv_scan_data(self, string_num):
        """Read IV scan data for a specific string from inverter
        
        Reads voltage and current data for the specified string.
        Returns list of (voltage, current) tuples for 64 data points.
        
        Args:
            string_num: String number (1-8)
        
        Returns:
            List of (voltage, current) tuples, or empty list on error
        """
        if not self.connected or not self.master:
            self.logger.warning("get_iv_scan_data: not connected")
            return []
        
        try:
            _get_iv = getattr(self.reg_module, 'get_iv_string_mapping', None)
            if _get_iv is None:
                from common.Solarize_PV_50kw_registers import get_iv_string_mapping as _get_iv

            # Get register mapping for this string
            mappings = _get_iv()
            string_info = None
            for m in mappings:
                if m['string_num'] == string_num:
                    string_info = m
                    break
            
            if not string_info:
                self.logger.warning(f"No mapping for string {string_num}")
                return []
            
            voltage_base = string_info['voltage_base']
            current_base = string_info['current_base']
            data_points = string_info['data_points']
            
            self.logger.info(f"Reading IV data: String {string_num}, V=0x{voltage_base:04X}, I=0x{current_base:04X}")
            
            # Read voltage data (64 registers)
            voltage_regs = self.master.read_holding_registers(voltage_base, data_points, self.slave_id)
            if not voltage_regs:
                self.logger.warning(f"Failed to read voltage for string {string_num}")
                return []
            
            # Read current data (64 registers)
            current_regs = self.master.read_holding_registers(current_base, data_points, self.slave_id)
            if not current_regs:
                self.logger.warning(f"Failed to read current for string {string_num}")
                return []
            
            # Build IV data list: (voltage, current) tuples as RAW values
            # Voltage: U16, scale 0.1V - pass raw value
            # Current: S16, scale 0.01A - pass raw value (server interprets sign)
            iv_data = []
            for i in range(data_points):
                voltage_raw = voltage_regs[i]   # U16 raw (0.1V unit)
                current_raw = current_regs[i]   # Raw value (server handles S16 interpretation)
                iv_data.append((voltage_raw, current_raw))
            
            # Debug: Print first and last values
            self.logger.info(f"IV READ String {string_num}: first=({iv_data[0][0]}, {iv_data[0][1]}), last=({iv_data[-1][0]}, {iv_data[-1][1]})")
            
            self.logger.info(f"IV data for string {string_num}: {len(iv_data)} points")
            return iv_data
            
        except Exception as e:
            self.logger.error(f"get_iv_scan_data error: {e}")
            return []
    
    def read_monitor_data(self):
        """Read power monitoring data (for H05 Body Type 14)
        
        Reads DEA-AVM registers (0x03E8-0x03FD) as a single block for control result monitoring.
        
        Returns dict with:
        - current_r/s/t: float (A)
        - voltage_rs/st/tr: float (V)
        - active_power_kw: float (kW)
        - reactive_power_var: float (Var)
        - power_factor: float
        - frequency: float (Hz)
        - status_flags: int
        """
        if not self.connected or not self.master:
            self.logger.warning(f"read_monitor_data: not connected (connected={self.connected}, master={self.master is not None})")
            return None
        
        try:
            # Guard: DEA registers may not exist in register module
            dea_start = getattr(self.RegMap, 'DEA_L1_CURRENT',
                                getattr(self.RegMap, 'DEA_L1_CURRENT_LOW', None))
            if dea_start is None:
                self.logger.debug("read_monitor_data: DEA registers not available")
                return None

            # Read entire DEA block at once (0x03E8 ~ 0x03FD = 22 registers)
            result = self._read_reg(dea_start, 22)

            if not result or len(result) < 22:
                self.logger.warning(f"read_monitor_data: DEA block read failed (got {len(result) if result else 0}/22)")
                return None
            
            # Parse the block
            # Helper to combine two 16-bit registers into signed 32-bit
            def to_s32(low, high):
                raw = (high << 16) | low
                if raw > 0x7FFFFFFF:
                    raw = raw - 0x100000000
                return raw
            
            data = {}
            
            # Phase Currents (scale 0.1A)
            # Offset: 0=L1_LOW, 1=L1_HIGH, 2=L2_LOW, 3=L2_HIGH, 4=L3_LOW, 5=L3_HIGH
            data['current_r'] = to_s32(result[0], result[1]) / 10.0
            data['current_s'] = to_s32(result[2], result[3]) / 10.0
            data['current_t'] = to_s32(result[4], result[5]) / 10.0
            
            # Phase Voltages (scale 0.1V)
            # Offset: 6=V1_LOW, 7=V1_HIGH, 8=V2_LOW, 9=V2_HIGH, 10=V3_LOW, 11=V3_HIGH
            data['voltage_rs'] = to_s32(result[6], result[7]) / 10.0
            data['voltage_st'] = to_s32(result[8], result[9]) / 10.0
            data['voltage_tr'] = to_s32(result[10], result[11]) / 10.0
            
            # Active Power (scale 0.1kW)
            # Offset: 12=P_LOW, 13=P_HIGH
            data['active_power_kw'] = to_s32(result[12], result[13]) / 10.0
            
            # Reactive Power (scale 1 Var)
            # Offset: 14=Q_LOW, 15=Q_HIGH
            data['reactive_power_var'] = to_s32(result[14], result[15])
            
            # Power Factor (scale 0.001)
            # Offset: 16=PF_LOW, 17=PF_HIGH
            data['power_factor'] = to_s32(result[16], result[17]) / 1000.0
            
            # Frequency (scale 0.1Hz)
            # Offset: 18=F_LOW, 19=F_HIGH
            data['frequency'] = to_s32(result[18], result[19]) / 10.0
            
            # Status Flags
            # Offset: 20=STS_LOW, 21=STS_HIGH
            data['status_flags'] = to_s32(result[20], result[21])
            
            self.logger.debug(f"DEA read OK: P={data['active_power_kw']:.1f}kW, I={data['current_r']:.1f}A")
            return data
            
        except Exception as e:
            self.logger.error(f"Read monitor data error: {e}")
            return None
    
    def read_model_info(self):
        """Read inverter model information"""
        if not hasattr(self.RegMap, 'DEVICE_MODEL'):
            return {'model': '', 'serial': '', 'mppt_count': 4,
                    'string_count': 8, 'nominal_power': 50000}
        if not self.connected or not self.master:
            return None
        
        # Inter-register delay for stable communication (100ms)
        REG_READ_DELAY = 0.1
        
        try:
            info = {}
            
            # Model name (16 registers = 32 bytes)
            time.sleep(REG_READ_DELAY)
            result = self.master.read_holding_registers(self.RegMap.DEVICE_MODEL, 16, self.slave_id)
            if result:
                model_bytes = b''
                for reg in result:
                    model_bytes += bytes([(reg >> 8) & 0xFF, reg & 0xFF])
                info['model'] = model_bytes.rstrip(b'\x00').decode('utf-8', errors='ignore')
            else:
                info['model'] = ''

            # Serial number (8 registers = 16 bytes)
            time.sleep(REG_READ_DELAY)
            result = self.master.read_holding_registers(self.RegMap.SERIAL_NUMBER, 8, self.slave_id)
            if result:
                serial_bytes = b''
                for reg in result:
                    serial_bytes += bytes([(reg >> 8) & 0xFF, reg & 0xFF])
                info['serial'] = serial_bytes.rstrip(b'\x00').decode('utf-8', errors='ignore')
            else:
                info['serial'] = ''
            
            # MPPT count
            time.sleep(REG_READ_DELAY)
            result = self.master.read_holding_registers(self.RegMap.MPPT_COUNT, 1, self.slave_id)
            info['mppt_count'] = result[0] if result else 4

            # String count (managed via config file, use default)
            info['string_count'] = 8

            # Nominal power
            time.sleep(REG_READ_DELAY)
            result_low = self.master.read_holding_registers(self.RegMap.NOMINAL_POWER_LOW, 1, self.slave_id)
            time.sleep(REG_READ_DELAY)
            result_high = self.master.read_holding_registers(self.RegMap.NOMINAL_POWER_HIGH, 1, self.slave_id)
            if result_low and result_high:
                info['nominal_power'] = self.reg_to_u32(result_low[0], result_high[0])
            else:
                info['nominal_power'] = 50000
            
            return info
            
        except Exception as e:
            self.logger.error(f"Read model info error: {e}")
            return None
    
    def read_device_info(self):
        """Read complete inverter device information

        Reads all device info registers for saving to file.

        Returns:
            dict: Complete device information or None on failure
        """
        if not hasattr(self.RegMap, 'DEVICE_MODEL'):
            return {'model_name': '', 'serial_number': '', 'mppt_count': 0,
                    'nominal_power': 0, 'read_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'slave_id': self.slave_id}
        if not self.connected or not self.master:
            return None
        
        # Inter-register delay for stable communication (100ms)
        REG_READ_DELAY = 0.1
        
        try:
            info = {}
            
            def read_string(addr, count):
                time.sleep(REG_READ_DELAY)
                result = self.master.read_holding_registers(addr, count, self.slave_id)
                if result:
                    data = b''
                    for reg in result:
                        data += bytes([(reg >> 8) & 0xFF, reg & 0xFF])
                    return data.rstrip(b'\x00').decode('utf-8', errors='ignore').strip()
                return ''
            
            def read_u16(addr):
                time.sleep(REG_READ_DELAY)
                result = self.master.read_holding_registers(addr, 1, self.slave_id)
                return result[0] if result else 0
            
            # 1. Device Model name (0x1A00, 8 regs)
            info['model_name'] = read_string(self.RegMap.DEVICE_MODEL, 8)

            # 2. Device Serial number (0x1A10, 8 regs)
            info['serial_number'] = read_string(self.RegMap.SERIAL_NUMBER, 8)

            # 3. Master firmware version (0x1A1C, 3 regs)
            info['master_firmware'] = read_string(self.RegMap.MASTER_FIRMWARE_VERSION, 3)

            # 4. Slave firmware version (0x1A26, 3 regs)
            info['slave_firmware'] = read_string(self.RegMap.SLAVE_FIRMWARE_VERSION, 3)

            # 5. MPPT Number (0x1A3B, 1 reg)
            info['mppt_count'] = read_u16(self.RegMap.MPPT_COUNT)

            # 6. Nominal Voltage (0x1A44, 1 reg, 0.1V)
            raw = read_u16(self.RegMap.NOMINAL_VOLTAGE)
            info['nominal_voltage'] = raw / 10.0  # V

            # 7. Nominal Frequency (0x1A45, 1 reg, 0.01Hz)
            raw = read_u16(self.RegMap.NOMINAL_FREQUENCY)
            info['nominal_frequency'] = raw / 100.0  # Hz

            # 8-9. Nominal Active Power (0x1A46 low, 0x1A4E high)
            low = read_u16(self.RegMap.NOMINAL_POWER_LOW)
            high = read_u16(self.RegMap.NOMINAL_POWER_HIGH)
            info['nominal_power'] = self.reg_to_u32(low, high)  # W

            # 10. Grid Phase Number (0x1A48, 1 reg)
            phase = read_u16(self.RegMap.GRID_PHASE_NUMBER)
            phase_names = {1: 'single', 2: 'split', 3: 'three'}
            info['grid_phase'] = phase
            info['grid_phase_name'] = phase_names.get(phase, f'unknown({phase})')

            # 11. EMS Firmware Version (0x1A60, 3 regs)
            info['ems_firmware'] = read_string(self.RegMap.EMS_FIRMWARE_VERSION, 3)

            # 12. LCD Firmware Version (0x1A8E, 3 regs)
            info['lcd_firmware'] = read_string(self.RegMap.LCD_FIRMWARE_VERSION, 3)
            
            # Add read timestamp
            info['read_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
            info['slave_id'] = self.slave_id
            
            return info
            
        except Exception as e:
            self.logger.error(f"Read device info error: {e}")
            return None
    
    def read_relay_data(self, slave_id: int = None):
        """Read KDU-300 protection relay data via Modbus
        
        Uses Holding Registers (Function Code 03) for simulator compatibility
        
        Args:
            slave_id: Modbus slave ID (uses self.slave_id if None)
        
        Returns:
            dict: Relay data for H01 body, or None on failure
        """
        if not self.connected or not self.master:
            return None
        
        if slave_id is None:
            slave_id = self.slave_id
        
        try:
            data = {}
            
            # Read block 1: Phase voltage V1,V2,V3 + Current A1,A2,A3 (addr 6-17, 12 regs)
            result = self.master.read_holding_registers(KDU300RegisterMap.V1, 12, slave_id)
            if result and len(result) >= 12:
                data['r_voltage'] = registers_to_float(result[0], result[1])   # V1 (addr 6-7)
                data['s_voltage'] = registers_to_float(result[2], result[3])   # V2 (addr 8-9)
                data['t_voltage'] = registers_to_float(result[4], result[5])   # V3 (addr 10-11)
                data['r_current'] = registers_to_float(result[6], result[7])   # A1 (addr 12-13)
                data['s_current'] = registers_to_float(result[8], result[9])   # A2 (addr 14-15)
                data['t_current'] = registers_to_float(result[10], result[11]) # A3 (addr 16-17)
            else:
                self.logger.error("Failed to read relay voltage/current")
                return None
            
            # Read block 2: Active power W1,W2,W3,Total (addr 18-25, 8 regs)
            result = self.master.read_holding_registers(KDU300RegisterMap.W1, 8, slave_id)
            if result and len(result) >= 8:
                data['r_active_power'] = registers_to_float(result[0], result[1])     # W1 (addr 18-19)
                data['s_active_power'] = registers_to_float(result[2], result[3])     # W2 (addr 20-21)
                data['t_active_power'] = registers_to_float(result[4], result[5])     # W3 (addr 22-23)
                data['total_active_power'] = registers_to_float(result[6], result[7]) # Total W (addr 24-25)
            else:
                self.logger.error("Failed to read relay power")
                return None
            
            # Read block 3: Avg PF, Frequency (addr 48-51, 4 regs)
            result = self.master.read_holding_registers(KDU300RegisterMap.AVG_PF, 4, slave_id)
            if result and len(result) >= 4:
                data['avg_power_factor'] = registers_to_float(result[0], result[1])  # Avg PF (addr 48-49)
                data['frequency'] = registers_to_float(result[2], result[3])         # Hz (addr 50-51)
            else:
                self.logger.error("Failed to read relay PF/frequency")
                return None
            
            # Read block 4: Energy +Wh, -Wh (addr 52-55, 4 regs)
            result = self.master.read_holding_registers(KDU300RegisterMap.POSITIVE_WH, 4, slave_id)
            if result and len(result) >= 4:
                data['received_energy'] = registers_to_float(result[0], result[1])  # +Wh (addr 52-53)
                data['sent_energy'] = registers_to_float(result[2], result[3])      # -Wh (addr 54-55)
            else:
                self.logger.error("Failed to read relay energy")
                return None
            
            # Read block 5: DO status (addr 92, 1 reg)
            result = self.master.read_holding_registers(KDU300RegisterMap.DO_STATUS, 1, slave_id)
            if result and len(result) >= 1:
                data['do_status'] = result[0]
            else:
                data['do_status'] = 0
            
            # Read block 6: DI1 status (addr 98, 1 reg)
            result = self.master.read_holding_registers(KDU300RegisterMap.DI1, 1, slave_id)
            if result and len(result) >= 1:
                data['di_status'] = result[0]
            else:
                data['di_status'] = 0
            
            self.logger.debug(f"Relay data: V={data['r_voltage']:.1f}/{data['s_voltage']:.1f}/{data['t_voltage']:.1f}V, "
                             f"I={data['r_current']:.2f}/{data['s_current']:.2f}/{data['t_current']:.2f}A, "
                             f"P={data['total_active_power']:.1f}W, PF={data['avg_power_factor']:.3f}")
            
            return data
            
        except Exception as e:
            self.logger.error(f"Read relay data error: {e}")
            return None
    
    def read_weather_data(self, slave_id: int = None):
        """Read SEM5046 weather station data via Modbus
        
        Uses Holding Registers (Function Code 03) for simulator compatibility
        Real SEM5046 uses FC04, but simulator uses FC03
        
        Args:
            slave_id: Modbus slave ID (uses self.slave_id if None)
        
        Returns:
            dict: Weather data for H01 body, or None on failure
        """
        if not self.connected or not self.master:
            return None
        
        if slave_id is None:
            slave_id = self.slave_id
        
        try:
            data = {}
            
            # Read block 1: Air temp, humidity, pressure, wind speed, direction (addr 1-5, 5 regs)
            result = self.master.read_holding_registers(SEM5046RegisterMap.AIR_TEMP, 5, slave_id)
            if result and len(result) >= 5:
                data['air_temp'] = raw_to_air_temp(result[0])           # 0x0001
                data['air_humidity'] = raw_to_humidity(result[1])       # 0x0002
                data['air_pressure'] = raw_to_pressure(result[2])       # 0x0003
                data['wind_speed'] = raw_to_wind_speed(result[3])       # 0x0004
                data['wind_direction'] = raw_to_wind_direction(result[4]) # 0x0005
            else:
                self.logger.error("Failed to read weather basic data")
                return None
            
            # Read block 2: Module temp 1, Horizontal radiation, accum (addr 6-8, 3 regs)
            result = self.master.read_holding_registers(SEM5046RegisterMap.MODULE_TEMP_1, 3, slave_id)
            if result and len(result) >= 3:
                data['module_temp_1'] = raw_to_module_temp(result[0])   # 0x0006
                data['horizontal_radiation'] = result[1]                # 0x0007 (W/m²)
                data['horizontal_accum'] = raw_to_accum_radiation(result[2])  # 0x0008
            else:
                self.logger.error("Failed to read weather radiation data")
                return None
            
            # Read block 3: Inclined radiation, accum (addr 13-14, 2 regs)
            result = self.master.read_holding_registers(SEM5046RegisterMap.INCLINED_RADIATION, 2, slave_id)
            if result and len(result) >= 2:
                data['inclined_radiation'] = result[0]                  # 0x000D (W/m²)
                data['inclined_accum'] = raw_to_accum_radiation(result[1])  # 0x000E
            else:
                self.logger.error("Failed to read weather inclined radiation")
                return None
            
            # Read block 4: Module temp 2,3,4 (addr 17-19, 3 regs)
            result = self.master.read_holding_registers(SEM5046RegisterMap.MODULE_TEMP_2, 3, slave_id)
            if result and len(result) >= 3:
                data['module_temp_2'] = raw_to_module_temp(result[0])   # 0x0011
                data['module_temp_3'] = raw_to_module_temp(result[1])   # 0x0012
                data['module_temp_4'] = raw_to_module_temp(result[2])   # 0x0013
            else:
                # Optional - use defaults if not available
                data['module_temp_2'] = data['module_temp_1']
                data['module_temp_3'] = data['module_temp_1']
                data['module_temp_4'] = data['module_temp_1']
            
            self.logger.debug(f"Weather data: Rad={data['horizontal_radiation']}W/m², "
                             f"Temp={data['air_temp']:.1f}℃, Module={data['module_temp_1']:.1f}℃")
            
            return data
            
        except Exception as e:
            self.logger.error(f"Read weather data error: {e}")
            return None


class ModbusHandlerCM4(ModbusHandlerHAT):
    """
    Modbus RTU Master using CM4 native UART (pyserial).
    Inherits all Modbus read/write methods from ModbusHandlerHAT.
    Only connect() and disconnect() differ (native UART instead of SPI HAT).
    """

    VERSION = "3.0.0"

    def __init__(self, channel: int = 0, baudrate: int = 9600, slave_id: int = 1,
                 port: str = None, reg_module=None):
        """
        Initialize CM4 Modbus handler.

        Args:
            channel: RS485 channel number (0=COM0, 1=COM1, 2=COM2, 3=COM3)
            baudrate: Communication speed (default 9600)
            slave_id: Modbus slave ID
            port: Override serial port path (default: auto from channel)
            reg_module: Dynamic register module (None=solarize_registers)
        """
        super().__init__(channel=channel, baudrate=baudrate, slave_id=slave_id,
                         reg_module=reg_module)
        self._port = port

    def connect(self):
        """Connect via CM4 native UART (pyserial)"""
        if not CM4_SERIAL_AVAILABLE:
            self.logger.error("CM4 serial driver not available")
            return False

        try:
            port = self._port or get_serial_port(self.channel)

            self.rs485_channel = RS485ChannelSerial(
                port=port,
                baudrate=self.baudrate,
                channel_num=self.channel
            )

            self.master = CM4ModbusMaster(
                self.rs485_channel, self.slave_id, timeout=1.0
            )
            self.master.set_retry_config(max_retries=3, base_delay=0.1, auto_retry=True)
            self.connected = True
            self.logger.info(
                f"Connected to CM4 COM{self.channel} ({port}) "
                f"@ {self.baudrate}bps (Slave {self.slave_id})"
            )
            return True

        except Exception as e:
            self.logger.error(f"CM4 serial connection error: {e}")
            # Cleanup partially created resources
            if hasattr(self, 'rs485_channel') and self.rs485_channel:
                try:
                    self.rs485_channel.close()
                except Exception:
                    pass
                self.rs485_channel = None
            return False

    def disconnect(self):
        """Disconnect from CM4 serial port"""
        if self.master:
            self.logger.info(f"Final Modbus stats: {self._get_stats_summary()}")

        self.connected = False
        if self.rs485_channel and hasattr(self.rs485_channel, 'close'):
            self.rs485_channel.close()


class ModbusHandlerSerial:
    """Modbus RTU Master using pymodbus (standard serial)"""

    def __init__(self, port: str = '/dev/ttyUSB0', baudrate: int = 9600, slave_id: int = 1,
                 reg_module=None, shared_client=None):
        self.port = port
        self.baudrate = baudrate
        self.slave_id = slave_id
        self.client = None
        self.connected = False
        self.logger = logging.getLogger(__name__)
        self._shared_client = shared_client

        # Dynamic register module binding
        _init_reg_attrs(self, reg_module)

        self._sim_energy = 1000000  # Initial 1000kWh
        self._sim_start = time.time()

        # HAT 호환 통계 속성 (read_inverter_data_dynamic에서 참조)
        self._read_count = 0
        self._last_stats_log = 0
        self._stats_log_interval = 100

    def connect(self):
        """Connect via pymodbus"""
        if self._shared_client is not None:
            self.client = self._shared_client
            self.master = PymodbusAdapter(self.client)
            self.connected = True
            self.logger.info(f"Serial: reusing shared client for {self.port} (slave={self.slave_id})")
            return True
        if not PYMODBUS_AVAILABLE:
            self.logger.error("pymodbus not available")
            return False
        
        try:
            self.client = ModbusSerialClient(
                port=self.port,
                baudrate=self.baudrate,
                bytesize=8,
                parity='N',
                stopbits=1,
                timeout=1
            )
            
            if self.client.connect():
                self.connected = True
                self.logger.info(f"Connected to {self.port} @ {self.baudrate}bps")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            return False
    
    def disconnect(self):
        """Disconnect"""
        if self.client:
            self.client.close()
        self.connected = False
    
    def _read_reg(self, addr, count=1):
        """Read holding registers (FC03) via pymodbus, returns list of U16 values or None."""
        result = self.client.read_holding_registers(addr, count, slave=self.slave_id)
        if result.isError():
            return None
        return result.registers

    def _read_input_reg(self, addr, count=1):
        """Read input registers (FC04) via pymodbus, returns list of U16 values or None."""
        result = self.client.read_input_registers(addr, count, slave=self.slave_id)
        if result.isError():
            return None
        return result.registers

    def _read_typed_value(self, field_name, addr):
        """Read a register value using the correct data type from DATA_TYPES.
        U32/S32는 u32_word_order에 따라 워드 순서 결정.
        """
        dtype = (self.reg_data_types or {}).get(field_name, 'u16')

        if dtype == 'float32':
            result = self._read_reg(addr, 2)
            if result and len(result) >= 2:
                w0, w1 = result[0], result[1]
                if getattr(self, 'u32_word_order', 'LH') == 'HL':
                    w0, w1 = w1, w0
                return self.reg_to_float32(w0, w1)
            return None
        elif dtype in ('u32', 's32'):
            result = self._read_reg(addr, 2)
            if result and len(result) >= 2:
                w0, w1 = result[0], result[1]
                if getattr(self, 'u32_word_order', 'LH') == 'HL':
                    lo, hi = w1, w0
                else:
                    lo, hi = w0, w1
                if dtype == 'u32':
                    return self.reg_to_u32(lo, hi)
                else:
                    val = self.reg_to_u32(lo, hi)
                    return val - 0x100000000 if val >= 0x80000000 else val
            return None
        elif dtype == 's16':
            result = self._read_reg(addr, 1)
            if result:
                v = result[0]
                return v - 65536 if v > 32767 else v
            return None
        else:  # u16
            result = self._read_reg(addr, 1)
            if result:
                return result[0]
            return None

    def _read_block(self, start, count, fc=3):
        """Read contiguous register block (FC03/FC04) — HAT 호환."""
        if fc == 4:
            return self._read_input_reg(start, count)
        return self._read_reg(start, count)

    def _build_reg_cache(self, read_blocks):
        """Read all READ_BLOCKS and return {addr: value} cache."""
        return ModbusHandlerHAT._build_reg_cache(self, read_blocks)

    def _get_typed_from_cache(self, field_name, addr, cache):
        """Extract typed value from register cache — HAT 로직 위임."""
        return ModbusHandlerHAT._get_typed_from_cache(self, field_name, addr, cache)

    def _check_stats_log(self):
        """통계 로깅 — HAT 호환."""
        self._read_count += 1

    def _read_inverter_data_dynamic(self):
        """범용 RegisterMap 기반 인버터 데이터 읽기 — HAT 로직 위임."""
        return ModbusHandlerHAT._read_inverter_data_dynamic(self)

    def _read_inverter_data_blocks(self):
        """READ_BLOCKS/DATA_PARSER 기반 블록 읽기 (Serial 버전 — HAT 로직 위임)."""
        return ModbusHandlerHAT._read_inverter_data_blocks(self)

    def _format_h01_from_raw(self, physical):
        """물리값 → H01 형식 변환 (Serial 버전 — HAT 로직 위임)."""
        return ModbusHandlerHAT._format_h01_from_raw(self, physical)

    def read_control_status(self):
        """DER-AVM 제어 상태 읽기 — HAT 로직 위임."""
        return ModbusHandlerHAT.read_control_status(self)

    def read_monitor_data(self):
        """DER-AVM 모니터링 데이터 읽기 — HAT 로직 위임."""
        return ModbusHandlerHAT.read_monitor_data(self)

    def write_control(self, control_type: int, value: int):
        """제어 명령 쓰기 — HAT 로직 위임."""
        return ModbusHandlerHAT.write_control(self, control_type, value)

    def read_inverter_data(self):
        """Read inverter data via pymodbus (same logic as HAT version)"""
        # 1순위: Block read
        if getattr(self, 'use_block_read', False):
            return self._read_inverter_data_blocks()
        # 2순위: Dynamic read
        if getattr(self, 'use_dynamic_read', False):
            return self._read_inverter_data_dynamic()

        if not self.connected:
            return None

        try:
            data = {}

            # Read L1 Phase (0x1001-0x1005)
            result = self.client.read_holding_registers(0x1001, 5, slave=self.slave_id)
            if result.isError():
                return None
            regs = result.registers
            data['r_voltage'] = int(regs[0] / 10)      # 0.1V register -> V (Scale 1)
            data['r_current'] = int(regs[1] / 10)      # 0.01A register -> 0.1A (for H01)
            data['frequency'] = int(regs[4] / 10)      # 0.01Hz -> 0.1Hz (Scale 10)
            
            # Read L2 Phase (0x1006-0x100A)
            result = self.client.read_holding_registers(0x1006, 5, slave=self.slave_id)
            if not result.isError():
                regs = result.registers
                data['s_voltage'] = int(regs[0] / 10)  # 0.1V register -> V (Scale 1)
                data['s_current'] = int(regs[1] / 10)  # 0.01A register -> 0.1A (for H01)
            
            # Read L3 Phase (0x100B-0x100F)
            result = self.client.read_holding_registers(0x100B, 5, slave=self.slave_id)
            if not result.isError():
                regs = result.registers
                data['t_voltage'] = int(regs[0] / 10)  # 0.1V register -> V (Scale 1)
                data['t_current'] = int(regs[1] / 10)  # 0.01A register -> 0.1A (for H01)
            
            # Read MPPT data
            mppt_data = []
            
            # MPPT 1-3 (0x1010-0x101B)
            result = self.client.read_holding_registers(0x1010, 12, slave=self.slave_id)
            if not result.isError():
                regs = result.registers
                for i in range(3):
                    idx = i * 4
                    mppt_data.append({
                        'voltage': regs[idx],       # Raw 0.1V register value
                        'current': regs[idx + 1]    # Raw 0.01A register value
                    })
            
            # MPPT 4 (0x103E-0x1041)
            result = self.client.read_holding_registers(0x103E, 4, slave=self.slave_id)
            if not result.isError():
                regs = result.registers
                mppt_data.append({
                    'voltage': regs[0],       # Raw 0.1V register value
                    'current': regs[1]        # Raw 0.01A register value
                })
            
            data['mppt'] = mppt_data
            
            # Calculate PV voltage/current from MPPT data
            # PV voltage = average of connected MPPTs (>= 100V = 1000 raw in 0.1V)
            # PV current = sum of all MPPT currents
            if mppt_data:
                connected = [m for m in mppt_data if m['voltage'] >= 1000]
                data['pv_voltage'] = int(sum(m['voltage'] for m in connected) / len(connected) / 10) if connected else 0
                data['pv_current'] = int(sum(m['current'] for m in mppt_data) / 10)
            else:
                data['pv_voltage'] = 0
                data['pv_current'] = 0

            # Read String data (0x1050-0x105F) - 8 strings x 2 regs (V,I pairs)
            result = self.client.read_holding_registers(0x1050, 16, slave=self.slave_id)
            if not result.isError():
                regs = result.registers
                # Extract only current values (odd indices: 1,3,5,7,9,11,13,15)
                # Raw 0.01A register values, will be converted in protocol_handler
                data['strings'] = [regs[i] for i in range(1, 16, 2)]
            else:
                data['strings'] = []
            
            # Read PV Power (0x1048-0x1049)
            result = self.client.read_holding_registers(0x1048, 2, slave=self.slave_id)
            if not result.isError():
                regs = result.registers
                data['pv_power'] = int(self.reg_to_u32(regs[0], regs[1]) * self.scale['power'])
            else:
                data['pv_power'] = 0

            # Read Grid Power (0x1037-0x1038)
            result = self.client.read_holding_registers(0x1037, 2, slave=self.slave_id)
            if not result.isError():
                regs = result.registers
                data['ac_power'] = int(self.reg_to_u32(regs[0], regs[1]) * self.scale['power'])
            else:
                data['ac_power'] = 0
            
            # Read Power Factor (0x103D)
            result = self.client.read_holding_registers(0x103D, 1, slave=self.slave_id)
            if not result.isError():
                pf = result.registers[0]
                if pf > 32767:
                    pf = pf - 65536
                data['power_factor'] = pf
            else:
                data['power_factor'] = 1000
            
            # Read Status (0x101C-0x1020)
            result = self.client.read_holding_registers(0x101C, 5, slave=self.slave_id)
            if not result.isError():
                regs = result.registers
                mode = regs[1]
                data['mode'] = mode  # Store raw mode value for logging
                if mode == self.InvMode.ON_GRID:
                    data['status'] = INV_STATUS_ON_GRID
                elif mode in (self.InvMode.STANDBY, self.InvMode.INITIAL,
                              self.InvMode.SHUTDOWN):
                    data['status'] = INV_STATUS_STANDBY
                elif mode == self.InvMode.FAULT:
                    data['status'] = INV_STATUS_FAULT
                else:
                    data['status'] = INV_STATUS_STANDBY
                data['alarm1'] = regs[2] if len(regs) > 2 else 0
                data['alarm2'] = regs[3] if len(regs) > 3 else 0
                data['alarm3'] = regs[4] if len(regs) > 4 else 0
            else:
                data['mode'] = self.InvMode.ON_GRID
                data['status'] = INV_STATUS_ON_GRID
                data['alarm1'] = data['alarm2'] = data['alarm3'] = 0

            # Read Energy (0x1021-0x1022)
            result = self.client.read_holding_registers(0x1021, 2, slave=self.slave_id)
            if not result.isError():
                regs = result.registers
                data['cumulative_energy'] = self.reg_to_u32(regs[0], regs[1]) * 1000
            else:
                data['cumulative_energy'] = 0
            
            return data
            
        except Exception as e:
            self.logger.error(f"Read error: {e}")
            return None
    
    def write_control(self, control_type: int, value: int):
        """Write control to inverter via pymodbus"""
        if not self.connected:
            return False
        
        try:
            reg_map = {
                CTRL_INV_ON_OFF: getattr(self.RegMap, 'INVERTER_ON_OFF', None),
                CTRL_INV_ACTIVE_POWER: getattr(self.RegMap, 'ACTIVE_POWER_PCT', None),
                CTRL_INV_POWER_FACTOR: getattr(self.RegMap, 'POWER_FACTOR_SET', None),
                CTRL_INV_REACTIVE_POWER: getattr(self.RegMap, 'REACTIVE_POWER_SET', None),
                CTRL_INV_IV_SCAN: getattr(self.RegMap, 'IV_SCAN_COMMAND', None),
            }

            if control_type == CTRL_INV_CONTROL_INIT:
                # Control Init: Reset all control values
                # ON_OFF: 0=Run(ON), 1=Stop(OFF) in register
                results = []
                if getattr(self.RegMap, 'INVERTER_ON_OFF', None) is not None:
                    results.append(self.client.write_register(
                        self.RegMap.INVERTER_ON_OFF, 0, slave=self.slave_id))
                if getattr(self.RegMap, 'ACTIVE_POWER_PCT', None) is not None:
                    results.append(self.client.write_register(
                        self.RegMap.ACTIVE_POWER_PCT, 1000, slave=self.slave_id))
                if getattr(self.RegMap, 'POWER_FACTOR_SET', None) is not None:
                    results.append(self.client.write_register(
                        self.RegMap.POWER_FACTOR_SET, 1000, slave=self.slave_id))
                if getattr(self.RegMap, 'REACTIVE_POWER_SET', None) is not None:
                    results.append(self.client.write_register(
                        self.RegMap.REACTIVE_POWER_SET, 0, slave=self.slave_id))
                return all(not r.isError() for r in results) if results else True
            
            reg = reg_map.get(control_type)
            if reg:
                result = self.client.write_register(reg, value, slave=self.slave_id)
                return not result.isError()
            return False
        except:
            return False
    
    def read_model_info(self):
        """Read inverter model information via pymodbus"""
        if not self.connected:
            return None

        # Guard: register module must have device info registers
        if not hasattr(self.RegMap, 'DEVICE_MODEL'):
            return {'model': '', 'serial': '', 'mppt_count': 4,
                    'string_count': 8, 'nominal_power': 50000}

        try:
            info = {}

            # Model name (16 registers = 32 bytes)
            result = self.client.read_holding_registers(self.RegMap.DEVICE_MODEL, 16, slave=self.slave_id)
            if not result.isError():
                model_bytes = b''
                for reg in result.registers:
                    model_bytes += bytes([(reg >> 8) & 0xFF, reg & 0xFF])
                info['model'] = model_bytes.rstrip(b'\x00').decode('utf-8', errors='ignore')
            else:
                info['model'] = ''

            # Serial number (8 registers = 16 bytes)
            result = self.client.read_holding_registers(self.RegMap.SERIAL_NUMBER, 8, slave=self.slave_id)
            if not result.isError():
                serial_bytes = b''
                for reg in result.registers:
                    serial_bytes += bytes([(reg >> 8) & 0xFF, reg & 0xFF])
                info['serial'] = serial_bytes.rstrip(b'\x00').decode('utf-8', errors='ignore')
            else:
                info['serial'] = ''

            # MPPT count
            result = self.client.read_holding_registers(self.RegMap.MPPT_COUNT, 1, slave=self.slave_id)
            info['mppt_count'] = result.registers[0] if not result.isError() else 4

            # String count (managed via config file, use default)
            info['string_count'] = 8

            # Nominal power
            result_low = self.client.read_holding_registers(self.RegMap.NOMINAL_POWER_LOW, 1, slave=self.slave_id)
            result_high = self.client.read_holding_registers(self.RegMap.NOMINAL_POWER_HIGH, 1, slave=self.slave_id)
            if not result_low.isError() and not result_high.isError():
                info['nominal_power'] = self.reg_to_u32(result_low.registers[0], result_high.registers[0])
            else:
                info['nominal_power'] = 50000
            
            return info
            
        except Exception as e:
            self.logger.error(f"Read model info error: {e}")
            return None
    
    def read_device_info(self):
        """Read complete inverter device information via pymodbus

        Returns:
            dict: Complete device information or None on failure
        """
        if not self.connected:
            return None

        # Guard: register module must have device info registers
        if not hasattr(self.RegMap, 'DEVICE_MODEL'):
            return {'model_name': '', 'serial_number': '', 'mppt_count': 0,
                    'nominal_power': 0, 'read_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'slave_id': self.slave_id}

        try:
            info = {}

            def read_string(addr, count):
                result = self.client.read_holding_registers(addr, count, slave=self.slave_id)
                if not result.isError():
                    data = b''
                    for reg in result.registers:
                        data += bytes([(reg >> 8) & 0xFF, reg & 0xFF])
                    return data.rstrip(b'\x00').decode('utf-8', errors='ignore').strip()
                return ''

            def read_u16(addr):
                result = self.client.read_holding_registers(addr, 1, slave=self.slave_id)
                return result.registers[0] if not result.isError() else 0

            # 1. Device Model name (0x1A00, 8 regs)
            info['model_name'] = read_string(self.RegMap.DEVICE_MODEL, 8)

            # 2. Device Serial number (0x1A10, 8 regs)
            info['serial_number'] = read_string(self.RegMap.SERIAL_NUMBER, 8)

            # 3. Master firmware version (0x1A1C, 3 regs)
            info['master_firmware'] = read_string(self.RegMap.MASTER_FIRMWARE_VERSION, 3)

            # 4. Slave firmware version (0x1A26, 3 regs)
            info['slave_firmware'] = read_string(self.RegMap.SLAVE_FIRMWARE_VERSION, 3)

            # 5. MPPT Number (0x1A3B, 1 reg)
            info['mppt_count'] = read_u16(self.RegMap.MPPT_COUNT)

            # 6. Nominal Voltage (0x1A44, 1 reg, 0.1V)
            raw = read_u16(self.RegMap.NOMINAL_VOLTAGE)
            info['nominal_voltage'] = raw / 10.0  # V

            # 7. Nominal Frequency (0x1A45, 1 reg, 0.01Hz)
            raw = read_u16(self.RegMap.NOMINAL_FREQUENCY)
            info['nominal_frequency'] = raw / 100.0  # Hz

            # 8-9. Nominal Active Power (0x1A46 low, 0x1A4E high)
            low = read_u16(self.RegMap.NOMINAL_POWER_LOW)
            high = read_u16(self.RegMap.NOMINAL_POWER_HIGH)
            info['nominal_power'] = self.reg_to_u32(low, high)  # W

            # 10. Grid Phase Number (0x1A48, 1 reg)
            phase = read_u16(self.RegMap.GRID_PHASE_NUMBER)
            phase_names = {1: 'single', 2: 'split', 3: 'three'}
            info['grid_phase'] = phase
            info['grid_phase_name'] = phase_names.get(phase, f'unknown({phase})')

            # 11. EMS Firmware Version (0x1A60, 3 regs)
            info['ems_firmware'] = read_string(self.RegMap.EMS_FIRMWARE_VERSION, 3)

            # 12. LCD Firmware Version (0x1A8E, 3 regs)
            info['lcd_firmware'] = read_string(self.RegMap.LCD_FIRMWARE_VERSION, 3)
            
            # Add read timestamp
            info['read_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
            info['slave_id'] = self.slave_id
            
            return info
            
        except Exception as e:
            self.logger.error(f"Read device info error: {e}")
            return None
    
    def read_relay_data(self, slave_id: int = None):
        """Read KDU-300 protection relay data via Modbus
        
        Uses Holding Registers (Function Code 03) for simulator compatibility
        
        Args:
            slave_id: Modbus slave ID (uses self.slave_id if None)
        
        Returns:
            dict: Relay data for H01 body, or None on failure
        """
        if not self.connected or not self.client:
            return None
        
        if slave_id is None:
            slave_id = self.slave_id
        
        try:
            data = {}
            
            # Read block 1: Phase voltage V1,V2,V3 + Current A1,A2,A3 (addr 6-17, 12 regs)
            result = self.client.read_holding_registers(KDU300RegisterMap.V1, 12, slave=slave_id)
            if not result.isError() and len(result.registers) >= 12:
                data['r_voltage'] = registers_to_float(result.registers[0], result.registers[1])
                data['s_voltage'] = registers_to_float(result.registers[2], result.registers[3])
                data['t_voltage'] = registers_to_float(result.registers[4], result.registers[5])
                data['r_current'] = registers_to_float(result.registers[6], result.registers[7])
                data['s_current'] = registers_to_float(result.registers[8], result.registers[9])
                data['t_current'] = registers_to_float(result.registers[10], result.registers[11])
            else:
                self.logger.error("Failed to read relay voltage/current")
                return None
            
            # Read block 2: Active power W1,W2,W3,Total (addr 18-25, 8 regs)
            result = self.client.read_holding_registers(KDU300RegisterMap.W1, 8, slave=slave_id)
            if not result.isError() and len(result.registers) >= 8:
                data['r_active_power'] = registers_to_float(result.registers[0], result.registers[1])
                data['s_active_power'] = registers_to_float(result.registers[2], result.registers[3])
                data['t_active_power'] = registers_to_float(result.registers[4], result.registers[5])
                data['total_active_power'] = registers_to_float(result.registers[6], result.registers[7])
            else:
                self.logger.error("Failed to read relay power")
                return None
            
            # Read block 3: Avg PF, Frequency (addr 48-51, 4 regs)
            result = self.client.read_holding_registers(KDU300RegisterMap.AVG_PF, 4, slave=slave_id)
            if not result.isError() and len(result.registers) >= 4:
                data['avg_power_factor'] = registers_to_float(result.registers[0], result.registers[1])
                data['frequency'] = registers_to_float(result.registers[2], result.registers[3])
            else:
                self.logger.error("Failed to read relay PF/frequency")
                return None
            
            # Read block 4: Energy +Wh, -Wh (addr 52-55, 4 regs)
            result = self.client.read_holding_registers(KDU300RegisterMap.POSITIVE_WH, 4, slave=slave_id)
            if not result.isError() and len(result.registers) >= 4:
                data['received_energy'] = registers_to_float(result.registers[0], result.registers[1])
                data['sent_energy'] = registers_to_float(result.registers[2], result.registers[3])
            else:
                self.logger.error("Failed to read relay energy")
                return None
            
            # Read block 5: DO status (addr 92, 1 reg)
            result = self.client.read_holding_registers(KDU300RegisterMap.DO_STATUS, 1, slave=slave_id)
            if not result.isError() and len(result.registers) >= 1:
                data['do_status'] = result.registers[0]
            else:
                data['do_status'] = 0
            
            # Read block 6: DI1 status (addr 98, 1 reg)
            result = self.client.read_holding_registers(KDU300RegisterMap.DI1, 1, slave=slave_id)
            if not result.isError() and len(result.registers) >= 1:
                data['di_status'] = result.registers[0]
            else:
                data['di_status'] = 0
            
            self.logger.debug(f"Relay data: V={data['r_voltage']:.1f}/{data['s_voltage']:.1f}/{data['t_voltage']:.1f}V, "
                             f"I={data['r_current']:.2f}/{data['s_current']:.2f}/{data['t_current']:.2f}A, "
                             f"P={data['total_active_power']:.1f}W, PF={data['avg_power_factor']:.3f}")
            
            return data
            
        except Exception as e:
            self.logger.error(f"Read relay data error: {e}")
            return None
    
    def read_weather_data(self, slave_id: int = None):
        """Read SEM5046 weather station data via Modbus
        
        Uses Holding Registers (Function Code 03) for simulator compatibility
        
        Args:
            slave_id: Modbus slave ID (uses self.slave_id if None)
        
        Returns:
            dict: Weather data for H01 body, or None on failure
        """
        if not self.connected or not self.client:
            return None
        
        if slave_id is None:
            slave_id = self.slave_id
        
        try:
            data = {}
            
            # Read block 1: Air temp, humidity, pressure, wind speed, direction (addr 1-5, 5 regs)
            result = self.client.read_holding_registers(SEM5046RegisterMap.AIR_TEMP, 5, slave=slave_id)
            if not result.isError() and len(result.registers) >= 5:
                data['air_temp'] = raw_to_air_temp(result.registers[0])
                data['air_humidity'] = raw_to_humidity(result.registers[1])
                data['air_pressure'] = raw_to_pressure(result.registers[2])
                data['wind_speed'] = raw_to_wind_speed(result.registers[3])
                data['wind_direction'] = raw_to_wind_direction(result.registers[4])
            else:
                self.logger.error("Failed to read weather basic data")
                return None
            
            # Read block 2: Module temp 1, Horizontal radiation, accum (addr 6-8, 3 regs)
            result = self.client.read_holding_registers(SEM5046RegisterMap.MODULE_TEMP_1, 3, slave=slave_id)
            if not result.isError() and len(result.registers) >= 3:
                data['module_temp_1'] = raw_to_module_temp(result.registers[0])
                data['horizontal_radiation'] = result.registers[1]
                data['horizontal_accum'] = raw_to_accum_radiation(result.registers[2])
            else:
                self.logger.error("Failed to read weather radiation data")
                return None
            
            # Read block 3: Inclined radiation, accum (addr 13-14, 2 regs)
            result = self.client.read_holding_registers(SEM5046RegisterMap.INCLINED_RADIATION, 2, slave=slave_id)
            if not result.isError() and len(result.registers) >= 2:
                data['inclined_radiation'] = result.registers[0]
                data['inclined_accum'] = raw_to_accum_radiation(result.registers[1])
            else:
                self.logger.error("Failed to read weather inclined radiation")
                return None
            
            # Read block 4: Module temp 2,3,4 (addr 17-19, 3 regs)
            result = self.client.read_holding_registers(SEM5046RegisterMap.MODULE_TEMP_2, 3, slave=slave_id)
            if not result.isError() and len(result.registers) >= 3:
                data['module_temp_2'] = raw_to_module_temp(result.registers[0])
                data['module_temp_3'] = raw_to_module_temp(result.registers[1])
                data['module_temp_4'] = raw_to_module_temp(result.registers[2])
            else:
                data['module_temp_2'] = data['module_temp_1']
                data['module_temp_3'] = data['module_temp_1']
                data['module_temp_4'] = data['module_temp_1']
            
            self.logger.debug(f"Weather data: Rad={data['horizontal_radiation']}W/m², "
                             f"Temp={data['air_temp']:.1f}℃, Module={data['module_temp_1']:.1f}℃")
            
            return data
            
        except Exception as e:
            self.logger.error(f"Read weather data error: {e}")
            return None


class _PymodbusStats:
    """PymodbusAdapter.get_stats() 반환용 더미 통계 객체.

    ModbusHandlerHAT._check_stats_log() 가 stats.successful 등을 참조하므로
    PC serial 모드에서도 AttributeError 없이 동작하도록 stub 제공.
    """
    def __init__(self):
        self.successful = 0
        self.total_requests = 0
        self.success_rate = 0.0
        self.timeouts = 0
        self.crc_errors = 0
        self.avg_response_time = 0.0


class PymodbusAdapter:
    """pymodbus SerialClient → ModbusMaster (HAT) API 래핑.

    KstarModbusHandlerSerial / HuaweiModbusHandlerSerial 에서
    self.master 속성으로 할당하여, HAT 기반 read/write 메서드(self.master.xxx)를
    PC USB-RS485 serial 모드에서도 변경 없이 사용할 수 있게 함.
    """

    def __init__(self, client):
        self._client = client
        # 내부 통계 (성공/실패 카운트)
        self._total = 0
        self._ok = 0

    def read_holding_registers(self, address: int, count: int, slave_id: int):
        """FC03 — 성공 시 list[int], 실패 시 None"""
        try:
            resp = self._client.read_holding_registers(address, count, slave=slave_id)
            if resp is None or resp.isError():
                self._total += 1
                return None
            self._total += 1
            self._ok += 1
            return resp.registers
        except Exception:
            self._total += 1
            return None

    def read_input_registers(self, address: int, count: int, slave_id: int):
        """FC04 — 성공 시 list[int], 실패 시 None"""
        try:
            resp = self._client.read_input_registers(address, count, slave=slave_id)
            if resp is None or resp.isError():
                self._total += 1
                return None
            self._total += 1
            self._ok += 1
            return resp.registers
        except Exception:
            self._total += 1
            return None

    def write_single_register(self, address: int, value: int, slave_id: int) -> bool:
        """FC06 — 성공 시 True"""
        try:
            resp = self._client.write_register(address, value, slave=slave_id)
            ok = resp is not None and not resp.isError()
            self._total += 1
            if ok:
                self._ok += 1
            return ok
        except Exception:
            self._total += 1
            return False

    def get_stats(self) -> _PymodbusStats:
        """_check_stats_log() 호환용 통계 객체 반환"""
        s = _PymodbusStats()
        s.total_requests = self._total
        s.successful = self._ok
        s.success_rate = (self._ok / self._total * 100.0) if self._total > 0 else 0.0
        return s


class ModbusHandlerTcp(ModbusHandlerSerial):
    """Modbus TCP master — 테스트 모드 전용.

    시뮬레이터(equipment_simulator.py --tcp-port)와 TCP 통신.
    ModbusHandlerSerial을 상속하여 read/write 메서드 재사용,
    connect()만 오버라이드하여 TcpClient 사용.
    """

    def __init__(self, host: str = '127.0.0.1', port: int = 5020, slave_id: int = 1,
                 reg_module=None, shared_client=None):
        # ModbusHandlerSerial init: fake port/baudrate
        super().__init__(port=f'tcp://{host}:{port}', baudrate=9600, slave_id=slave_id,
                         reg_module=reg_module, shared_client=shared_client)
        self._tcp_host = host
        self._tcp_port = port

    def connect(self):
        """TCP 연결 후 PymodbusAdapter로 self.master 설정."""
        if self._shared_client is not None:
            self.client = self._shared_client
            self.master = PymodbusAdapter(self.client)
            self.connected = True
            self.logger.info(f"TCP: reusing shared client (slave={self.slave_id})")
            return True
        if not PYMODBUS_AVAILABLE:
            self.logger.error("TCP: pymodbus not available")
            return False
        try:
            self.client = ModbusTcpClient(self._tcp_host, port=self._tcp_port, timeout=2)
            if self.client.connect():
                self.master = PymodbusAdapter(self.client)
                self.connected = True
                self.logger.info(f"TCP: connected to {self._tcp_host}:{self._tcp_port} (slave={self.slave_id})")
                return True
            self.logger.error(f"TCP: failed to connect {self._tcp_host}:{self._tcp_port}")
            return False
        except Exception as e:
            self.logger.error(f"TCP connect error: {e}")
            return False


class ModbusHandlerSimulation:
    """Simulation mode - no hardware"""
    
    def __init__(self, slave_id: int = 1, reg_module=None,
                 protocol: str = 'solarize', string_count: int = 8,
                 iv_scan_data_points: int = 64):
        self.slave_id = slave_id
        self.connected = False
        self.logger = logging.getLogger(__name__)
        self.protocol = protocol
        self.string_count = string_count
        self.iv_scan_data_points = iv_scan_data_points

        # Dynamic register module binding
        _init_reg_attrs(self, reg_module)

        self._sim_energy = 1000000  # Initial 1000kWh
        self._sim_start = time.time()

        # Nominal rating (50kW inverter)
        self.NOMINAL_POWER = 50000  # 50kW in W

        # Control state (using register convention: 0=ON/Run, 1=OFF/Stop)
        self._on_off = 0              # 0=ON(Run), 1=OFF(Stop)
        self._power_limit = 1000      # 100.0% (scale 0.1%)
        self._power_factor = 1000     # 1.000 (scale 0.001)
        self._reactive_power = 0      # 0% (scale 0.1%)
        self._iv_scan_status = 0
        self._iv_scan_data = {}
    
    def connect(self):
        self.connected = True
        self.logger.info("[SIM] Simulation mode enabled")
        return True
    
    def disconnect(self):
        self.connected = False
    
    def read_inverter_data(self):
        """Generate simulation data based on control values
        
        Power flow: PV (DC) → Inverter (98% efficiency) → AC Output
        - AC Power = PV Power × 0.98 (efficiency)
        - Power Cap = NOMINAL_POWER × (active_power_pct / 100)
        - Actual Output = min(AC Power, Power Cap)
        
        Example: PV produces 30kW, 50% limit (25kW cap)
        - Possible AC = 30kW × 0.98 = 29.4kW
        - Actual AC = min(29.4kW, 25kW) = 25kW (capped)
        """
        elapsed = time.time() - self._sim_start
        # Use abs() to keep sun always positive (no night time in simulation)
        sun = abs(math.sin(elapsed / 300 * math.pi))
        # Minimum sun factor to avoid zero power
        sun = max(0.3, sun)
        
        # Apply ON/OFF control (0=ON/Run, 1=OFF/Stop)
        if self._on_off == 1:  # OFF - no output
            sun = 0
        
        # Active power limit as % of nominal (upper cap)
        active_power_pct = self._power_limit / 10.0  # Convert from 0.1% scale to %
        power_cap = self.NOMINAL_POWER * (active_power_pct / 100.0)  # Upper limit (W)
        
        # PV side simulation (DC) - matches NOMINAL_POWER at sun=1.0
        pv_v = int(380 + sun * 100)  # 380-480V
        pv_c = int(sun * self.NOMINAL_POWER / 400)  # Current to match nominal power at ~400V
        pv_p = pv_v * pv_c  # DC power (W)
        
        # AC output = PV power × 98% efficiency, then apply cap
        possible_ac = pv_p * 0.98
        ac_p = int(min(possible_ac, power_cap))
        
        # Accumulate energy (Wh)
        self._sim_energy += ac_p / 3600.0
        
        # Phase current from AC power (A unit, same as SolarizeHandler)
        # P = √3 × V × I × PF, for single phase: I = P / V
        phase_current = int(ac_p / 3 / 380) if ac_p > 0 else 0
        
        # MPPT: raw register 형식 (voltage=0.1V, current=0.01A)
        mppt_count = getattr(self.reg_module, 'MPPT_CHANNELS', 4)
        mppt = []
        pv_per_mppt = pv_p / max(mppt_count, 1) if pv_p > 0 else 0
        for i in range(mppt_count):
            mppt_v_phys = 380 + i * 5 + sun * 20  # V (물리값)
            mppt_c_phys = pv_per_mppt / mppt_v_phys if mppt_v_phys > 0 else 0  # A
            mppt.append({
                'voltage': int(mppt_v_phys * 10),   # 0.1V raw
                'current': int(mppt_c_phys * 100),   # 0.01A raw
            })

        # String: raw 0.01A 단위
        str_count = getattr(self.reg_module, 'STRING_CHANNELS', 8)
        strings_per_mppt = max(str_count // max(mppt_count, 1), 1)
        strings = []
        for i in range(str_count):
            mppt_idx = min(i // strings_per_mppt, mppt_count - 1)
            str_c = mppt[mppt_idx]['current'] / strings_per_mppt if mppt_count > 0 else 0
            strings.append(int(str_c))
        
        return {
            'pv_voltage': pv_v,
            'pv_current': pv_c,
            'pv_power': pv_p,
            'r_voltage': 380,
            's_voltage': 380,
            't_voltage': 380,
            'r_current': phase_current,
            's_current': phase_current,
            't_current': phase_current,
            'ac_power': ac_p,
            'power_factor': self._power_factor,
            'frequency': 600,
            'cumulative_energy': int(self._sim_energy),
            'mode': self.InvMode.ON_GRID if self._on_off == 0 else self.InvMode.SHUTDOWN,
            'status': INV_STATUS_ON_GRID if self._on_off == 0 else 0x09,  # 0x09=Shutdown
            'alarm1': 0, 'alarm2': 0, 'alarm3': 0,
            'mppt': mppt,
            'strings': strings
        }
    
    def read_relay_data(self):
        """Generate relay simulation data"""
        v = 380 + random.uniform(-2, 2)
        i = 10 + random.uniform(-0.5, 0.5)
        p = v * i * 0.98
        
        return {
            'r_voltage': v, 's_voltage': v, 't_voltage': v,
            'r_current': i, 's_current': i, 't_current': i,
            'r_active_power': p, 's_active_power': p, 't_active_power': p,
            'total_active_power': p * 3,
            'avg_power_factor': 0.98,
            'frequency': 60.0,
            'received_energy': self._sim_energy * 1.1,
            'sent_energy': 0,
            'do_status': 1, 'di_status': 1
        }
    
    def read_weather_data(self):
        """Generate weather simulation data"""
        import math
        now = datetime.now()
        hour = now.hour + now.minute / 60.0
        
        # Solar radiation based on time
        if 6.0 <= hour <= 18.0:
            day_progress = (hour - 6.0) / 12.0
            radiation = 1000 * math.sin(day_progress * math.pi)
            radiation *= (1 + random.uniform(-0.15, 0.05))
            radiation = max(0, radiation)
        else:
            radiation = 0
        
        # Temperature based on time (min at 6am, max at 2pm)
        temp = 15 + 10 * math.sin((hour - 8) / 24 * 2 * math.pi)
        temp += random.uniform(-1, 1)
        
        # Module temperature
        module_temp = temp + (radiation / 1000.0) * 30.0
        
        return {
            'air_temp': temp,
            'air_humidity': 50 + random.uniform(-15, 15),
            'air_pressure': 1013 + random.uniform(-5, 5),
            'wind_speed': 2 + random.uniform(0, 5),
            'wind_direction': random.uniform(0, 360),
            'module_temp_1': module_temp + random.uniform(-2, 2),
            'horizontal_radiation': int(radiation),
            'horizontal_accum': 0.0,
            'inclined_radiation': int(radiation * 1.15),
            'inclined_accum': 0.0,
            'module_temp_2': module_temp + random.uniform(-2, 2),
            'module_temp_3': module_temp + random.uniform(-2, 2),
            'module_temp_4': module_temp + random.uniform(-2, 2),
        }
    
    def write_control(self, control_type: int, value: int):
        """Write control to simulated inverter
        
        Control types:
        - 14: CTRL_INV_CONTROL_INIT - Reset to PF=1.0, Reactive=0%, Active=100%
        - 15: CTRL_INV_ON_OFF - 0=ON, 1=OFF
        - 16: CTRL_INV_ACTIVE_POWER - 0~1100 (0.1% scale, 0~110%)
        - 17: CTRL_INV_POWER_FACTOR - -1000~1000 (0.001 scale)
        - 18: CTRL_INV_REACTIVE_POWER - % of nominal (0.1% scale)
        
        Note: ON_OFF uses register convention: 0=Run(ON), 1=Stop(OFF)
        """
        self.logger.info(f"[SIM] Control: type={control_type}, value={value}")
        
        if control_type == CTRL_INV_ON_OFF:
            self._on_off = value
            self.logger.info(f"[SIM] ON/OFF -> {'ON (On-Grid)' if value == 0 else 'OFF (Shutdown)'}")  # 0=ON, 1=OFF
            
        elif control_type == CTRL_INV_ACTIVE_POWER:
            self._power_limit = value
            self.logger.info(f"[SIM] Active Power -> {value/10:.1f}%")
            
        elif control_type == CTRL_INV_POWER_FACTOR:
            self._power_factor = value
            pf_val = value if value < 32768 else value - 65536
            self.logger.info(f"[SIM] Power Factor -> {pf_val/1000:.3f}")
            
        elif control_type == CTRL_INV_REACTIVE_POWER:
            self._reactive_power = value
            rp_val = value if value < 32768 else value - 65536
            self.logger.info(f"[SIM] Reactive Power -> {rp_val/10:.1f}%")
            
        elif control_type == CTRL_INV_IV_SCAN:
            if value == 1:
                self._start_iv_scan()
                
        elif control_type == CTRL_INV_CONTROL_INIT:
            # Reset to default: ON, PF=1.0, Reactive=0%, Active=100%
            # ON_OFF: 0=Run(ON), 1=Stop(OFF) in register convention
            self._on_off = 0              # 0=ON (Run)
            self._power_limit = 1000      # 100.0%
            self._power_factor = 1000     # 1.000
            self._reactive_power = 0      # 0%
            self.logger.info(f"[SIM] Control INIT: ON=0(Run), PF=1.000, Reactive=0%, Active=100%")
        
        return True
    
    def _start_iv_scan(self):
        """Start simulated IV scan using protocol-specific parameters"""
        def scan_thread():
            try:
                self._iv_scan_status = 1  # Running
                self._iv_scan_data.clear()
                n_strings = self.string_count
                n_points = self.iv_scan_data_points
                self.logger.info(f"[SIM] IV Scan thread started: {n_strings} strings, {n_points} points")
                # Generate all data first
                for string_num in range(1, n_strings + 1):
                    iv_data = []
                    voc = 750.0 + (string_num - 1) * 5
                    v_min = 200.0
                    isc = 12.0 + (string_num - 1) * 0.1
                    for i in range(n_points):
                        v = voc - (voc - v_min) * i / n_points
                        current = isc * (1 - math.exp(-5 * (1 - i / n_points)))
                        iv_data.append((v, current))
                    self._iv_scan_data[string_num] = iv_data
                self.logger.info(f"[SIM] IV Scan data generated: {len(self._iv_scan_data)} strings")
                # Simulate scan duration
                time.sleep(2.0)
                self._iv_scan_status = 2  # Complete
                self.logger.info(f"[SIM] IV Scan status -> FINISHED")
            except Exception as e:
                self.logger.error(f"[SIM] IV Scan thread error: {e}")
                self._iv_scan_status = 0

        threading.Thread(target=scan_thread, daemon=True).start()
    
    def read_control_status(self):
        """Read current control status (for H05 Body Type 13)"""
        pf = self._power_factor if self._power_factor < 32768 else self._power_factor - 65536
        
        return {
            'on_off': self._on_off,
            'power_factor': pf / 1000.0,
            'operation_mode': 0,
            'reactive_power_pct': self._reactive_power / 10.0,  # %
            'active_power_pct': self._power_limit / 10.0,  # %
            'iv_scan_status': self._iv_scan_status  # 0=Idle, 1=Running, 2=Finished
        }
    
    def read_monitor_data(self):
        """Read power monitoring data (for H05 Body Type 14)
        
        Uses same data as read_inverter_data() for consistency.
        H01 and H05(14) should show identical inverter state.
        
        Note: read_inverter_data() returns scaled values:
        - current: 0.01A scale (multiply by 100)
        - voltage: 0.1V scale (multiply by 10)
        - ac_power: W (no scale)
        - frequency: 0.01Hz scale
        
        This method returns actual physical values (A, V, kW, Hz).
        """
        # Get inverter data (same source as H01)
        inv_data = self.read_inverter_data()
        
        # Power factor
        pf = self._power_factor
        if pf >= 32768:
            pf = pf - 65536
        pf = pf / 1000.0
        
        # Active power in kW (inv_data['ac_power'] is in W)
        ac_power_w = inv_data['ac_power']
        active_power_kw = ac_power_w / 1000.0
        
        # Reactive power limit setting
        reactive_power_pct = self._reactive_power
        if reactive_power_pct >= 32768:
            reactive_power_pct = reactive_power_pct - 65536
        reactive_power_pct = reactive_power_pct / 10.0  # 0.1% -> %
        
        # P, Q, PF are interrelated:
        # - PF control mode: Q = P × tan(acos(PF))
        # - Q control mode: PF = P / sqrt(P² + Q²)
        
        if abs(reactive_power_pct) > 0.1:
            # Q control mode: reactive_power_pct defines Q cap
            q_cap = self.NOMINAL_POWER * (abs(reactive_power_pct) / 100.0)
            reactive_power_var = q_cap if reactive_power_pct >= 0 else -q_cap
            
            # Calculate PF from P and Q
            if active_power_kw > 0:
                p_w = active_power_kw * 1000
                s_va = math.sqrt(p_w * p_w + reactive_power_var * reactive_power_var)
                pf = p_w / s_va if s_va > 0 else 1.0
            else:
                pf = 1.0
        else:
            # PF control mode: calculate Q from PF
            if abs(pf) < 1.0 and active_power_kw > 0:
                reactive_power_var = active_power_kw * 1000 * math.tan(math.acos(min(abs(pf), 0.999)))
                if pf < 0:
                    reactive_power_var = -reactive_power_var
            else:
                reactive_power_var = 0
        
        # Convert from scaled values to actual physical values
        # current: 0.01A scale -> A
        # voltage: already in actual V (380)
        return {
            'current_r': inv_data['r_current'] / 100.0,  # 0.01A -> A
            'current_s': inv_data['s_current'] / 100.0,
            'current_t': inv_data['t_current'] / 100.0,
            'voltage_rs': inv_data['r_voltage'],  # Already in V
            'voltage_st': inv_data['s_voltage'],
            'voltage_tr': inv_data['t_voltage'],
            'active_power_kw': active_power_kw,
            'reactive_power_var': reactive_power_var,
            'power_factor': pf,
            'frequency': inv_data['frequency'] / 10.0,  # 0.1Hz -> Hz
            'status_flags': 0x0001 if self._on_off == 0 else 0x0000
        }
    
    def read_model_info(self):
        """Read simulated model info"""
        return {
            'model': 'SRPV-3-50-KS-SIM',
            'serial': 'SIM001234',
            'firmware': 'V1.4.0-SIM',
            'nominal_power': 50000,
            'mppt_count': 4,
            'string_count': 8
        }
    
    def read_device_info(self):
        """Read simulated device info (all fields)"""
        return {
            'model_name': 'SRPV-3-50-KS-SIM',
            'serial_number': 'SIM-001234567',
            'master_firmware': '140000',
            'slave_firmware': '140000',
            'mppt_count': 4,
            'nominal_voltage': 220.0,
            'nominal_frequency': 60.0,
            'nominal_power': self.NOMINAL_POWER,
            'grid_phase': 3,
            'grid_phase_name': 'three',
            'ems_firmware': '100000',
            'lcd_firmware': '100000',
            'read_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'slave_id': self.slave_id
        }
    
    def get_iv_scan_data(self, string_num):
        """Get IV scan data for a string"""
        return self._iv_scan_data.get(string_num, [])


class DeviceHandlerWrapper:
    """Wrapper for device handlers with unified read_data interface
    
    This wrapper provides a unified read_data() method that automatically
    calls the appropriate read method (read_inverter_data or read_relay_data)
    based on the device type.
    """
    
    def __init__(self, handler, device_type: str):
        """Initialize wrapper
        
        Args:
            handler: The underlying Modbus handler (HAT, Serial, or Simulation)
            device_type: 'inverter' or 'relay'
        """
        self.handler = handler
        self.device_type = device_type
    
    def read_data(self):
        """Read data from device using appropriate method
        
        Returns:
            dict: Device data or None on failure
        """
        if self.device_type == 'inverter':
            if hasattr(self.handler, 'read_inverter_data'):
                return self.handler.read_inverter_data()
        elif self.device_type == 'relay':
            if hasattr(self.handler, 'read_relay_data'):
                return self.handler.read_relay_data()
        return None
    
    def __getattr__(self, name):
        """Delegate all other attributes to the underlying handler"""
        return getattr(self.handler, name)


class WeatherHandlerWrapper:
    """Wrapper for weather sensor handlers with read_weather_data interface
    
    This wrapper provides a unified read_weather_data() method for weather sensors.
    """
    
    def __init__(self, handler):
        """Initialize wrapper
        
        Args:
            handler: The underlying Modbus handler (HAT, Serial, or Simulation)
        """
        self.handler = handler
    
    def read_weather_data(self):
        """Read weather data from sensor
        
        Returns:
            dict: Weather data or None on failure
        """
        if hasattr(self.handler, 'read_weather_data'):
            return self.handler.read_weather_data()
        return None
    
    def __getattr__(self, name):
        """Delegate all other attributes to the underlying handler"""
        return getattr(self.handler, name)


class MultiDeviceHandler:
    """Handle multiple devices with HAT / CM4 serial / pymodbus support"""

    def __init__(self, use_hat: bool = True, use_cm4: bool = False,
                 channel: int = 1, serial_port: str = '/dev/ttyUSB0',
                 baudrate: int = 9600, simulation_mode: bool = False,
                 tcp_host: str = None, tcp_port: int = None):
        self.use_hat = use_hat
        self.use_cm4 = use_cm4
        self.channel = channel
        self.serial_port = serial_port
        self.baudrate = baudrate
        self.simulation_mode = simulation_mode
        # Modbus TCP test mode
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.use_tcp = bool(tcp_host and tcp_port)
        self.handlers = {}
        self.logger = logging.getLogger(__name__)
        # PC serial 모드에서 첫 번째로 연결된 ModbusSerialClient 를 공유
        # (단일 COM 포트에 복수 핸들러가 각자 open 시도하는 문제 방지)
        self._shared_pc_client = None

        # Check availability (CM4 takes priority)
        if use_cm4 and not CM4_SERIAL_AVAILABLE:
            self.logger.warning("CM4 serial not available, falling back to simulation")
            self.simulation_mode = True
        elif use_hat and not use_cm4 and not HAT_AVAILABLE:
            self.logger.warning("HAT not available, falling back to simulation")
            self.simulation_mode = True
        elif not use_hat and not use_cm4 and not PYMODBUS_AVAILABLE:
            self.logger.warning("pymodbus not available, falling back to simulation")
            self.simulation_mode = True
    
    def _create_handler(self, slave_id: int, simulation: bool = False,
                        channel: int = None, baudrate: int = None):
        """Create appropriate Modbus handler based on platform.

        Args:
            slave_id: Modbus slave ID
            simulation: Force simulation mode
            channel: Override channel number
            baudrate: Override baudrate

        Returns:
            tuple: (handler, mode_string)
        """
        ch = channel if channel is not None else self.channel
        br = baudrate if baudrate is not None else self.baudrate
        use_simulation = simulation or self.simulation_mode

        if use_simulation:
            return ModbusHandlerSimulation(slave_id), "SIM"
        elif self.use_cm4:
            return ModbusHandlerCM4(ch, br, slave_id), "CM4"
        elif self.use_hat:
            return ModbusHandlerHAT(ch, br, slave_id), "HAT"
        else:
            return ModbusHandlerSerial(self.serial_port, br, slave_id), "Serial"

    def add_inverter(self, device_number: int, slave_id: int = 1, simulation: bool = False):
        """Add inverter

        Args:
            device_number: Device number for H01 packet
            slave_id: Modbus slave ID
            simulation: If True, use simulation mode regardless of global setting
        """
        key = (1, device_number)  # DEVICE_INVERTER = 1

        handler, mode = self._create_handler(slave_id, simulation)

        if handler.connect():
            self._save_shared_pc_client(handler)   # PC 모드: 첫 연결 클라이언트 저장
            self.handlers[key] = handler
            self.logger.info(f"Added INV{device_number} (slave={slave_id}, mode={mode})")
            return True
        return False

    def _save_shared_pc_client(self, handler):
        """PC serial 모드에서 최초 연결 성공 클라이언트를 공유 저장.

        이후 KstarModbusHandlerSerial / HuaweiModbusHandlerSerial 생성 시
        동일 COM 포트를 재open 하지 않고 공유하여 사용한다.
        """
        if (not self.use_hat and not self.use_cm4
                and self._shared_pc_client is None
                and hasattr(handler, 'client')
                and handler.client is not None):
            self._shared_pc_client = handler.client
            self.logger.info("PC serial: shared ModbusSerialClient saved")

    def add_relay(self, device_number: int, slave_id: int = 1, simulation: bool = False):
        """Add relay

        Args:
            device_number: Device number for H01 packet
            slave_id: Modbus slave ID
            simulation: If True, use simulation mode regardless of global setting
        """
        key = (4, device_number)  # DEVICE_PROTECTION_RELAY = 4

        handler, mode = self._create_handler(slave_id, simulation)

        if handler.connect():
            self._save_shared_pc_client(handler)
            self.handlers[key] = handler
            self.logger.info(f"Added RELAY{device_number} (slave={slave_id}, mode={mode})")
            return True
        return False

    def add_weather(self, device_number: int, slave_id: int = 1,
                    channel: int = 1, baudrate: int = 9600,
                    simulation: bool = False):
        """Add weather sensor (SEM5046) and return handler wrapper

        Args:
            device_number: Device number for H01 packet
            slave_id: Modbus slave ID
            channel: RS485 channel number
            baudrate: Serial baudrate
            simulation: If True, use simulation mode regardless of global setting

        Returns:
            WeatherHandlerWrapper object or None on failure
        """
        key = (5, device_number)  # DEVICE_WEATHER_STATION = 5

        handler, mode = self._create_handler(slave_id, simulation,
                                             channel=channel, baudrate=baudrate)

        if handler.connect():
            self._save_shared_pc_client(handler)
            self.handlers[key] = handler
            self.logger.info(f"Added WEATHER{device_number} (slave={slave_id}, mode={mode})")
            return WeatherHandlerWrapper(handler)

        self.logger.error(f"add_weather: Failed to connect WEATHER{device_number} slave={slave_id}")
        return None
    
    def add_device(self, device_type: str, slave_id: int = 1, protocol: str = 'modbus',
                   channel: int = 1, baudrate: int = 9600,
                   mppt_count: int = 4, string_count: int = 8,
                   simulation: bool = False, device_number: int = 1,
                   iv_scan_data_points: int = 64):
        """Add device and return handler for rtu_client.py compatibility
        
        Args:
            device_type: 'inverter' or 'relay'
            slave_id: Modbus slave ID
            protocol: Protocol type ('solarize', 'modbus', etc.)
            channel: RS485 channel number
            baudrate: Serial baudrate
            mppt_count: MPPT count (for inverters)
            string_count: String count (for inverters)
            simulation: If True, use simulation mode
            device_number: Device number for handler registration
            
        Returns:
            Handler object or None on failure
        """
        # Update channel and baudrate if different from init
        if channel != self.channel:
            self.channel = channel
        if baudrate != self.baudrate:
            self.baudrate = baudrate
        
        # Per-device simulation mode takes priority
        use_simulation = simulation or self.simulation_mode

        # 모든 프로토콜에 대해 동적 레지스터 모듈 로딩
        reg_mod = load_register_module(protocol)

        if use_simulation:
            handler = ModbusHandlerSimulation(
                slave_id, reg_module=reg_mod,
                protocol=protocol, string_count=string_count,
                iv_scan_data_points=iv_scan_data_points)
        elif self.use_tcp:
            handler = ModbusHandlerTcp(self.tcp_host, self.tcp_port, slave_id,
                                        reg_module=reg_mod,
                                        shared_client=self._shared_pc_client)
        elif self.use_cm4:
            handler = ModbusHandlerCM4(channel, baudrate, slave_id, reg_module=reg_mod)
        elif self.use_hat:
            handler = ModbusHandlerHAT(channel, baudrate, slave_id, reg_module=reg_mod)
        else:
            handler = ModbusHandlerSerial(self.serial_port, baudrate, slave_id,
                                          reg_module=reg_mod,
                                          shared_client=self._shared_pc_client)

        if handler.connect():
            self._save_shared_pc_client(handler)   # PC 모드: 첫 연결 클라이언트 저장
            mode = "SIM" if use_simulation else ("TCP" if self.use_tcp else ("CM4" if self.use_cm4 else ("HAT" if self.use_hat else "Serial")))
            self.logger.info(f"add_device: {device_type}/{protocol} slave={slave_id}, mode={mode}")

            # Register handler in handlers dict for read_monitor_data etc.
            if device_type == 'inverter':
                key = (1, device_number)  # DEVICE_INVERTER = 1
            elif device_type == 'relay':
                key = (4, device_number)  # DEVICE_PROTECTION_RELAY = 4
            else:
                key = (0, device_number)
            self.handlers[key] = handler
            self.logger.info(f"Registered handler: key={key}")

            return DeviceHandlerWrapper(handler, device_type)

        self.logger.error(f"add_device: Failed to connect {device_type} slave={slave_id}")
        return None
    
    def read_data(self, device_type: int, device_number: int):
        """Read device data"""
        key = (device_type, device_number)
        handler = self.handlers.get(key)
        
        if not handler:
            return None
        
        if device_type == 1:
            return handler.read_inverter_data()
        elif device_type == 4:
            if hasattr(handler, 'read_relay_data'):
                return handler.read_relay_data()
        return None
    
    def write_control(self, device_type: int, device_number: int,
                      control_type: int, value: int):
        """Write control"""
        key = (device_type, device_number)
        handler = self.handlers.get(key)
        
        if handler:
            return handler.write_control(control_type, value)
        return False
    
    def read_control_status(self, device_type: int, device_number: int):
        """Read control status from device"""
        key = (device_type, device_number)
        handler = self.handlers.get(key)
        
        if handler and hasattr(handler, 'read_control_status'):
            return handler.read_control_status()
        return None
    
    def read_model_info(self, device_type: int, device_number: int):
        """Read model info from device"""
        key = (device_type, device_number)
        handler = self.handlers.get(key)
        
        if handler and hasattr(handler, 'read_model_info'):
            return handler.read_model_info()
        return None
    
    def read_device_info(self, device_type: int, device_number: int):
        """Read complete device info from device
        
        Returns:
            dict: Complete device information or None on failure
        """
        key = (device_type, device_number)
        handler = self.handlers.get(key)
        
        if handler and hasattr(handler, 'read_device_info'):
            return handler.read_device_info()
        return None
    
    def get_iv_scan_data(self, device_type: int, device_number: int, string_num: int):
        """Get IV scan data from device"""
        key = (device_type, device_number)
        handler = self.handlers.get(key)
        
        if handler and hasattr(handler, 'get_iv_scan_data'):
            return handler.get_iv_scan_data(string_num)
        return []
    
    def read_monitor_data(self, device_type: int, device_number: int):
        """Read power monitoring data from device (for H05 Body Type 14)"""
        key = (device_type, device_number)
        handler = self.handlers.get(key)
        
        if handler and hasattr(handler, 'read_monitor_data'):
            return handler.read_monitor_data()
        self.logger.warning(f"read_monitor_data: No handler for ({device_type}, {device_number}), available: {list(self.handlers.keys())}")
        return None
    
    def disconnect_all(self):
        """Disconnect all"""
        for handler in self.handlers.values():
            handler.disconnect()
        self.handlers.clear()
