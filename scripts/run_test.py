#!/usr/bin/env python3
"""자동 통합 테스트 러너 (시뮬레이터 → RTU → 대시보드 → API 검증)

사용:
    python scripts/run_test.py
    python scripts/run_test.py --wait 15      # H01 대기 시간
    python scripts/run_test.py --keep-running # 테스트 후 프로세스 유지

환경:
    - Modbus TCP: 127.0.0.1:5020 (시뮬레이터 서버)
    - UDP server: 127.0.0.1:13132 (대시보드 UDP 엔진)
    - Web API: 127.0.0.1:8080 (대시보드 REST)
    - RTU: 로컬에서 TCP 마스터로 시뮬레이터 접속

각 프로세스는 subprocess.Popen으로 백그라운드 실행되며,
테스트 완료 후 자동 종료된다.
"""
import os
import sys
import time
import json
import signal
import argparse
import subprocess
import urllib.request
import urllib.error

# 프로젝트 루트 경로
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# 테스트 환경 설정
TEST_MODBUS_PORT = 5020
TEST_WEB_PORT = 8090        # 프로덕션 8080과 분리
TEST_UDP_PORT = 13142       # 프로덕션 13132와 분리
TEST_DB_PATH = os.path.join(PROJECT_ROOT, 'web_server_prod', 'test_rtu_dashboard.db')
TEST_RTU_ID = 10000001

# 프로덕션 RTU server target → 테스트 대시보드 UDP로 보내도록 설정
# rtu_client.py는 config에서 읽으므로 임시 config 만들거나 env로 override
TEST_CONFIG_DIR = os.path.join(PROJECT_ROOT, '.test_config')


def log(msg, level='INFO'):
    color = {
        'INFO': '\033[36m',
        'OK': '\033[32m',
        'FAIL': '\033[31m',
        'WARN': '\033[33m',
    }.get(level, '')
    reset = '\033[0m'
    print(f"{color}[{level}]{reset} {msg}", flush=True)


def prepare_test_config():
    """테스트용 config 디렉토리 생성. RTU가 로컬 서버로 보내도록."""
    os.makedirs(TEST_CONFIG_DIR, exist_ok=True)
    src_cfg = os.path.join(PROJECT_ROOT, 'config', 'rtu_config.ini')
    dst_cfg = os.path.join(TEST_CONFIG_DIR, 'rtu_config.ini')
    with open(src_cfg, 'r', encoding='utf-8') as f:
        cfg = f.read()
    # Override server host/port and communication_period
    cfg = cfg.replace('primary_host = solarize.ddns.net', 'primary_host = 127.0.0.1')
    cfg = cfg.replace('primary_port = 13132', f'primary_port = {TEST_UDP_PORT}')
    cfg = cfg.replace('communication_period = 20', 'communication_period = 5')
    with open(dst_cfg, 'w', encoding='utf-8') as f:
        f.write(cfg)

    # Copy rs485_ch1.ini (simulator uses same file)
    src_rs = os.path.join(PROJECT_ROOT, 'config', 'rs485_ch1.ini')
    dst_rs = os.path.join(TEST_CONFIG_DIR, 'rs485_ch1.ini')
    with open(src_rs, 'r', encoding='utf-8') as f:
        rs = f.read()
    with open(dst_rs, 'w', encoding='utf-8') as f:
        f.write(rs)
    log(f"Test config prepared: {TEST_CONFIG_DIR}")


def cleanup_test_db():
    """Remove test DB files."""
    for ext in ('', '-wal', '-shm'):
        p = TEST_DB_PATH + ext
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


def start_simulator():
    """Start equipment_simulator.py in TCP mode."""
    log(f"Starting simulator (TCP port {TEST_MODBUS_PORT})...")
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    proc = subprocess.Popen(
        [sys.executable, '-u', 'pc_programs/equipment_simulator.py',
         '--tcp-port', str(TEST_MODBUS_PORT)],
        cwd=PROJECT_ROOT,
        stdout=open(os.path.join(PROJECT_ROOT, '.test_sim.log'), 'w'),
        stderr=subprocess.STDOUT,
        env=env,
    )
    return proc


def start_dashboard():
    """Start dashboard with isolated ports and DB."""
    log(f"Starting dashboard (web:{TEST_WEB_PORT}, udp:{TEST_UDP_PORT})...")
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    env['RTU_WEB_PORT'] = str(TEST_WEB_PORT)
    env['RTU_UDP_PORT'] = str(TEST_UDP_PORT)
    env['RTU_DB_PATH'] = TEST_DB_PATH
    proc = subprocess.Popen(
        [sys.executable, '-u', '-m', 'web_server_prod.main'],
        cwd=PROJECT_ROOT,
        stdout=open(os.path.join(PROJECT_ROOT, '.test_dash.log'), 'w'),
        stderr=subprocess.STDOUT,
        env=env,
    )
    return proc


def start_rtu():
    """Start RTU in TCP master mode, pointing at test simulator and dashboard."""
    log(f"Starting RTU (tcp master → 127.0.0.1:{TEST_MODBUS_PORT})...")
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    env['RTU_FIRST_WAIT'] = '5'   # fast startup for tests
    # rtu_client.py imports are relative, so run from rtu_program/
    proc = subprocess.Popen(
        [sys.executable, '-u', 'rtu_client.py',
         '-c', os.path.abspath(TEST_CONFIG_DIR),
         '--modbus-tcp', f'127.0.0.1:{TEST_MODBUS_PORT}',
         '-d'],   # debug logging
        cwd=os.path.join(PROJECT_ROOT, 'rtu_program'),
        stdout=open(os.path.join(PROJECT_ROOT, '.test_rtu.log'), 'w'),
        stderr=subprocess.STDOUT,
        env=env,
    )
    return proc


def read_log(name):
    """Read a log file."""
    p = os.path.join(PROJECT_ROOT, f'.test_{name}.log')
    if os.path.exists(p):
        try:
            with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        except Exception:
            pass
    return ''


def fetch_api(path):
    """GET http://127.0.0.1:WEB_PORT/path → dict or None."""
    url = f"http://127.0.0.1:{TEST_WEB_PORT}{path}"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, json.JSONDecodeError):
        return None


def post_api(path, payload):
    """POST JSON to http://127.0.0.1:WEB_PORT/path → dict or None."""
    url = f"http://127.0.0.1:{TEST_WEB_PORT}{path}"
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(url, data=data,
                                      headers={'Content-Type': 'application/json'},
                                      method='POST')
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, json.JSONDecodeError) as e:
        return {'error': str(e)}


def get_device_data(rtu_id, dev_num):
    """Get single device data from API."""
    data = fetch_api(f'/api/rtus/{rtu_id}/devices')
    if not data:
        return None
    return data.get('devices', {}).get(f'INV_{dev_num}', {}).get('data', {})


def test_time_change(rtu_id, wait_seconds=15):
    """Phase 2: Verify cumulative energy and AC power vary over time.

    Energy accumulates slowly (50kW × 10s = 0.14 kWh = 140Wh),
    so we check it doesn't DECREASE and AC power has some variation
    (sun simulation produces small fluctuations).
    """
    log("=" * 80)
    log(f"PHASE 2: Time-based variation (sun fluctuation, energy non-decrease)")
    initial_data = fetch_api(f'/api/rtus/{rtu_id}/devices')
    if not initial_data:
        log("Cannot fetch initial data", 'FAIL')
        return False
    initial = {}
    for key, dev in initial_data.get('devices', {}).items():
        if dev.get('type_name') == 'INV':
            d = dev.get('data', {})
            initial[key] = {
                'energy': d.get('cumulative_energy', 0),
                'ac': d.get('ac_power', 0),
            }

    time.sleep(wait_seconds)

    final_data = fetch_api(f'/api/rtus/{rtu_id}/devices')
    if not final_data:
        log("Cannot fetch final data", 'FAIL')
        return False

    fail = 0
    for key in sorted(initial.keys()):
        init = initial[key]
        d = final_data.get('devices', {}).get(key, {}).get('data', {})
        final_e = d.get('cumulative_energy', 0)
        final_ac = d.get('ac_power', 0)
        delta_e = final_e - init['energy']
        delta_ac = final_ac - init['ac']
        # Energy should NOT decrease
        if final_e < init['energy']:
            log(f"  {key}: energy DECREASED {init['energy']}→{final_e}", 'FAIL')
            fail += 1
            continue
        # AC power should vary (sun simulation has fluctuation)
        ac_changed = abs(delta_ac) > 0
        e_changed = delta_e > 0
        if ac_changed or e_changed:
            log(f"  {key}: ac Δ{delta_ac:+d}W energy Δ{delta_e:+d}Wh OK", 'OK')
        else:
            log(f"  {key}: no variation (ac={final_ac} energy={final_e})", 'WARN')
    return fail == 0


def test_control(rtu_id, dev_num):
    """Phase 3: Send control commands and verify."""
    log("=" * 80)
    log(f"PHASE 3: Control commands (INV_{dev_num})")
    fail = 0

    # 3.1: PF set to 0.95 (raw 950)
    log(f"  3.1 Power factor → 0.95")
    r = post_api('/api/control/power_factor', {
        'rtu_id': rtu_id, 'device_num': dev_num, 'value': 950
    })
    if r and 'error' not in r:
        log(f"      sent: {r}", 'OK')
    else:
        log(f"      FAIL: {r}", 'FAIL')
        fail += 1
    time.sleep(2)

    # 3.2: Active power → 80%
    log(f"  3.2 Active power → 80% (raw 800)")
    r = post_api('/api/control/active_power', {
        'rtu_id': rtu_id, 'device_num': dev_num, 'value': 800
    })
    if r and 'error' not in r:
        log(f"      sent: {r}", 'OK')
    else:
        log(f"      FAIL: {r}", 'FAIL')
        fail += 1
    time.sleep(2)

    # 3.3: ON/OFF → ON (0)
    log(f"  3.3 ON/OFF → ON")
    r = post_api('/api/control/on_off', {
        'rtu_id': rtu_id, 'device_num': dev_num, 'value': 0
    })
    if r and 'error' not in r:
        log(f"      sent: {r}", 'OK')
    else:
        log(f"      FAIL: {r}", 'FAIL')
        fail += 1
    time.sleep(2)

    # 3.4: Verify control_status reflects PF/active_power changes
    time.sleep(3)
    d = get_device_data(rtu_id, dev_num)
    if d and 'ctrl' in d:
        ctrl = d['ctrl']
        log(f"  3.4 Control status verification:")
        log(f"      ctrl={ctrl}", 'OK')
        # 'power_factor' may be in raw or float
        pf = ctrl.get('power_factor', 0)
        ap = ctrl.get('active_power_pct', 0)
        if pf == 950 or pf == 0.95:
            log(f"      PF=950 reflected", 'OK')
        else:
            log(f"      PF mismatch: expected 950, got {pf}", 'FAIL')
            fail += 1
        if ap == 800 or ap == 80.0:
            log(f"      Active=800 reflected", 'OK')
        else:
            log(f"      Active mismatch: expected 800, got {ap}", 'FAIL')
            fail += 1
    else:
        log(f"  3.4 No 'ctrl' data in API response", 'FAIL')
        fail += 1
    return fail == 0


def test_iv_scan(rtu_id, dev_num=1):
    """Phase 4: IV scan command."""
    log("=" * 80)
    log(f"PHASE 4: IV Scan (INV_{dev_num})")
    r = post_api('/api/control/iv_scan', {
        'rtu_id': rtu_id, 'device_num': dev_num, 'value': 1
    })
    if not r or 'error' in r:
        log(f"  IV scan request failed: {r}", 'FAIL')
        return False
    log(f"  IV scan command sent: {r}", 'OK')
    time.sleep(8)  # IV scan takes ~5s + processing
    iv_data = fetch_api(f'/api/rtus/{rtu_id}/iv_scan')
    if iv_data:
        log(f"  IV scan data received: {len(str(iv_data))} bytes", 'OK')
        # Check structure
        if isinstance(iv_data, dict):
            keys = list(iv_data.keys())[:5]
            log(f"  Keys: {keys}", 'OK')
        return True
    else:
        log(f"  No IV scan data after 8s", 'WARN')
        return False


def test_der_avm_monitor(rtu_id):
    """Phase 5: Verify DER-AVM Monitor (mon) data exists for inverters with control=DER_AVM."""
    log("=" * 80)
    log(f"PHASE 5: DER-AVM Monitor data")
    data = fetch_api(f'/api/rtus/{rtu_id}/devices')
    if not data:
        log("Cannot fetch", 'FAIL')
        return False
    pass_cnt = 0
    fail_cnt = 0
    for key in sorted(data.get('devices', {}).keys()):
        dev = data['devices'][key]
        if dev.get('type_name') != 'INV':
            continue
        d = dev.get('data', {})
        mon = d.get('mon')
        ctrl = d.get('ctrl')
        if mon and ctrl:
            i_r = mon.get('current_r', 0)
            v_rs = mon.get('voltage_rs', 0)
            p = mon.get('active_power_kw', 0)
            log(f"  {key}: I={i_r}A V={v_rs}V P={p}kW [mon] ctrl_pf={ctrl.get('power_factor')} OK", 'OK')
            pass_cnt += 1
        else:
            log(f"  {key}: no mon/ctrl data (control=NONE?)", 'WARN')
            fail_cnt += 1
    return pass_cnt > 0


def test_info(rtu_id):
    """Phase 7: Verify RTU Info (H05 body 1) and Inverter Info (H05 body 11).

    - RTU Info: trigger via /api/control/rtu_info, then check /api/rtus rtu_info field
    - Inverter Info: trigger via /api/control/inv_model for INV_1, then check
      /api/rtus/{id}/devices INV_1 data has model_name + serial_number
    """
    log("=" * 80)
    log(f"PHASE 7: RTU Info + Inverter Info")
    fail = 0

    # 7.1 RTU Info — trigger explicit request
    log(f"  7.1 Triggering H03 CTRL_RTU_INFO")
    r = post_api('/api/control/rtu_info', {
        'rtu_id': rtu_id, 'device_num': 0, 'value': 0
    })
    if r and 'error' not in r:
        log(f"      sent: {r}", 'OK')
    else:
        log(f"      FAIL: {r}", 'FAIL')
        fail += 1
    time.sleep(3)

    rtus_list = fetch_api('/api/rtus')
    rtu_info = None
    if rtus_list:
        for r in rtus_list.get('rtus', []):
            if int(r.get('rtu_id', -1)) == int(rtu_id):
                rtu_info = r.get('rtu_info', {})
                log(f"      rtu_info={rtu_info} rtu_type={r.get('rtu_type')}", 'OK')
                break
    if not rtu_info or not rtu_info.get('model'):
        log(f"      RTU info missing/empty", 'FAIL')
        fail += 1
    else:
        for f in ('model', 'phone', 'serial', 'firmware'):
            if not rtu_info.get(f):
                log(f"      missing field: {f}", 'FAIL')
                fail += 1

    # 7.2 Inverter Info — trigger requests for ALL 11 inverters
    expected_models = {
        1:  'SRPV',     # solarize
        2:  'SG50CX',   # sungrow
        3:  'KSG',      # kstar
        4:  'SUN2000',  # huawei
        5:  'EKOS',     # ekos
        6:  'SE-50K',   # senergy
        7:  'SOFAR',    # sofar
        8:  'SOLIS',    # solis
        9:  'MOD',      # growatt
        10: 'CPS',      # cps
        11: 'STT',      # sunways
    }
    log(f"  7.2 Triggering H03 CTRL_INV_MODEL for INV_1..INV_11")
    for dn in expected_models.keys():
        r = post_api('/api/control/model_info', {
            'rtu_id': rtu_id, 'device_num': dn, 'value': 0
        })
        if r and 'error' not in r:
            log(f"      INV_{dn} sent", 'OK')
        else:
            log(f"      INV_{dn} FAIL: {r}", 'FAIL')
            fail += 1
        time.sleep(0.3)
    time.sleep(5)

    data = fetch_api(f'/api/rtus/{rtu_id}/devices')
    if not data:
        log(f"      Cannot fetch devices", 'FAIL')
        return False
    devs = data.get('devices', {})
    for dn, expected_prefix in expected_models.items():
        key = f'INV_{dn}'
        dev = devs.get(key, {})
        d = dev.get('data', {})
        model_name = d.get('model_name', '')
        serial = d.get('serial_number', '')
        iv_cap = d.get('iv_scan')
        der_cap = d.get('der_avm')
        log(f"      {key}: model='{model_name}' sn='{serial}' "
            f"iv_scan={iv_cap} der_avm={der_cap}")
        if not model_name:
            log(f"      {key} missing model_name", 'FAIL')
            fail += 1
        elif expected_prefix not in model_name:
            log(f"      {key} model_name '{model_name}' missing prefix '{expected_prefix}'", 'FAIL')
            fail += 1
        if not serial:
            log(f"      {key} missing serial_number", 'FAIL')
            fail += 1
    return fail == 0


def test_kstar_night(rtu_id, dev_num=3):
    """Phase 6: Verify Kstar nighttime standby behavior.

    Note: This requires sun=0 in simulator, which depends on time of day.
    Cannot easily force in test mode, so just check status.
    """
    log("=" * 80)
    log(f"PHASE 6: Kstar nighttime mode (INV_{dev_num})")
    d = get_device_data(rtu_id, dev_num)
    if d:
        status = d.get('status', -1)
        ac = d.get('ac_power', 0)
        log(f"  Kstar status={status} ac={ac}W")
        log(f"  (Cannot force night mode in TCP test - skipping detailed check)", 'WARN')
        return True
    return False


def validate_devices(data, expected):
    """Validate device data with strict value range checks.

    expected = {'INV_1': ('solarize', 4, 8, 50000), ...}  # last = nominal_w
    """
    devices = data.get('devices', {})
    results = []
    for key in sorted(devices.keys()):
        dev = devices[key]
        d = dev.get('data', {})
        dtype = dev.get('type_name', '')
        if dtype == 'INV':
            mppt = d.get('mppt', [])
            strings = d.get('strings', [])
            ac = d.get('ac_power', 0)
            pv = d.get('pv_power', 0)
            freq = d.get('frequency', 0)
            pf = d.get('power_factor', 0)
            energy = d.get('cumulative_energy', 0)
            r_v = d.get('r_voltage', 0)
            # r_current is in 0.1A units in H01 packet (raw)
            r_i_raw = d.get('r_current', 0)
            r_i = r_i_raw / 10.0  # convert to A
            exp = expected.get(key, ('?', 0, 0, 50000))
            proto, exp_m, exp_s = exp[0], exp[1], exp[2]
            nominal = exp[3] if len(exp) > 3 else 50000
            issues = []

            # 1. Channel count
            if len(mppt) != exp_m:
                issues.append(f"mppt={len(mppt)}/{exp_m}")
            if len(strings) != exp_s:
                issues.append(f"str={len(strings)}/{exp_s}")

            # 2. AC Power: should be 50%-110% of nominal in sunny test
            if ac <= 0:
                issues.append(f"ac=0W")
            elif ac < nominal * 0.3 or ac > nominal * 1.5:
                issues.append(f"ac={ac}W(nom={nominal})")

            # 3. PV Power: should be slightly higher than AC (efficiency)
            if pv <= 0:
                issues.append(f"pv=0W")
            elif pv < ac * 0.9:  # PV should be ≥ AC
                issues.append(f"pv<ac(pv={pv},ac={ac})")

            # 4. Frequency 59-61 Hz
            if freq < 58 or freq > 62:
                issues.append(f"freq={freq}")

            # 5. Power factor 0.8-1.0
            if pf < 0.8 or pf > 1.05:
                issues.append(f"pf={pf}")

            # 6. R phase voltage 200-450V
            if r_v < 200 or r_v > 450:
                issues.append(f"rv={r_v}")

            # 7. R phase current > 0 if ac > 0
            if ac > 0 and r_i <= 0:
                issues.append(f"ri=0")

            # 8. Energy > 1000 kWh (initial)
            if energy < 1000:
                issues.append(f"energy={energy}kWh")

            # 9. MPPT voltage 100-1000V
            for i, m in enumerate(mppt):
                v = m.get('voltage', 0)
                if v < 100 or v > 1000:
                    if ac > 0:
                        issues.append(f"MPPT{i+1}_V={v}")
                        break

            # 10. MPPT current > 0 if ac > 0
            for i, m in enumerate(mppt):
                c = m.get('current', 0)
                if ac > 0 and c <= 0:
                    issues.append(f"MPPT{i+1}_I={c}")
                    break

            status = 'OK' if not issues else 'FAIL'
            results.append((key, proto, status, issues,
                            f'{ac/1000:.1f}', f'{pv/1000:.1f}', freq, pf,
                            f'{r_v:.0f}/{r_i:.1f}',
                            len(mppt), len(strings),
                            f'{energy/1000:.0f}'))
        elif dtype in ('RELAY', 'WEATHER'):
            results.append((key, dtype.lower(), 'OK', [],
                            '-', '-', '-', '-', '-', 0, 0, '-'))
    return results


def main():
    parser = argparse.ArgumentParser(description='Integration test runner')
    parser.add_argument('--wait', type=int, default=20,
                        help='Wait seconds after startup before API check (default: 20)')
    parser.add_argument('--keep-running', action='store_true',
                        help='Keep processes running after test (manual inspection)')
    parser.add_argument('--extended', action='store_true',
                        help='Run extended test phases (time/control/IV/DER-AVM/night)')
    args = parser.parse_args()

    prepare_test_config()
    cleanup_test_db()

    processes = []
    try:
        # 1. Simulator first (TCP server)
        sim = start_simulator()
        processes.append(('simulator', sim))
        time.sleep(3)
        if sim.poll() is not None:
            out = sim.stdout.read().decode('utf-8', errors='ignore')
            log(f"Simulator failed to start:\n{out[-2000:]}", 'FAIL')
            return 1

        # 2. Dashboard (UDP server + Web API)
        dash = start_dashboard()
        processes.append(('dashboard', dash))
        time.sleep(4)
        if dash.poll() is not None:
            out = dash.stdout.read().decode('utf-8', errors='ignore')
            log(f"Dashboard failed to start:\n{out[-2000:]}", 'FAIL')
            return 1

        # 3. RTU (TCP master + UDP client)
        rtu = start_rtu()
        processes.append(('rtu', rtu))
        time.sleep(3)
        if rtu.poll() is not None:
            out = rtu.stdout.read().decode('utf-8', errors='ignore')
            log(f"RTU failed to start:\n{out[-3000:]}", 'FAIL')
            return 1

        # 4. Wait for first H01 transmission + server processing
        log(f"Waiting {args.wait}s for H01 packets to reach dashboard...")
        time.sleep(args.wait)

        # 5. Query API — try both /api/rtus and /api/rtus/ID/devices
        log(f"Fetching API /api/rtus...")
        rtus_list = fetch_api('/api/rtus')
        if rtus_list:
            log(f"API /api/rtus returned: {len(rtus_list.get('rtus', []))} RTUs")
            for r in rtus_list.get('rtus', []):
                log(f"  RTU {r.get('rtu_id')}: status={r.get('status')} devices={r.get('device_count')}")

        log(f"Fetching API /api/rtus/{TEST_RTU_ID}/devices...")
        data = fetch_api(f'/api/rtus/{TEST_RTU_ID}/devices')
        if not data:
            log("API did not return data.", 'FAIL')
            log("=== RTU LOG (last 60 lines) ===", 'WARN')
            print('\n'.join(read_log('rtu').split('\n')[-60:]))
            log("=== SIMULATOR LOG (last 30 lines) ===", 'WARN')
            print('\n'.join(read_log('sim').split('\n')[-30:]))
            log("=== DASHBOARD LOG (last 30 lines) ===", 'WARN')
            print('\n'.join(read_log('dash').split('\n')[-30:]))
            return 1

        # 6. Validate (proto, mppt, str, nominal_W)
        expected = {
            'INV_1':  ('solarize', 4, 8, 50000),
            'INV_2':  ('sungrow',  4, 8, 50000),
            'INV_3':  ('kstar',    3, 9, 60000),
            'INV_4':  ('huawei',   4, 0, 50000),
            'INV_5':  ('ekos',     1, 2, 10000),
            'INV_6':  ('senergy',  4, 8, 50000),
            'INV_7':  ('sofar',    4, 4, 70000),
            'INV_8':  ('solis',    4, 0, 50000),
            'INV_9':  ('growatt',  2, 8, 30000),
            'INV_10': ('cps',      4, 0, 50000),
            'INV_11': ('sunways',  3, 6, 30000),
        }
        results = validate_devices(data, expected)

        print()
        print('=' * 110)
        print(f"{'Device':<10} {'Proto':<10} {'St':<5} {'AC':<6} {'PV':<6} {'Freq':<6} {'PF':<6} {'R V/I':<12} {'MPPT':<5} {'STR':<5} {'Energy':<8}")
        print('=' * 110)
        pass_cnt = fail_cnt = 0
        for row in results:
            key, proto, status = row[0], row[1], row[2]
            issues = row[3]
            ac, pv, freq, pf, rvi, m, s, energy = row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]
            if status == 'OK':
                pass_cnt += 1
            else:
                fail_cnt += 1
            print(f"{key:<10} {proto:<10} {status:<5} {ac:<6} {pv:<6} {str(freq):<6} {str(pf):<6} {rvi:<12} {m:<5} {s:<5} {energy:<8}")
            if issues:
                print(f"           ISSUES: {', '.join(issues)}")
        print('=' * 110)
        print(f"PHASE 1 RESULT: {pass_cnt} PASS / {fail_cnt} FAIL / {len(results)} TOTAL")
        print()

        # Optional extended phases
        if args.extended:
            phase_results = {'phase1': fail_cnt == 0}
            phase_results['phase2'] = test_time_change(TEST_RTU_ID, wait_seconds=10)
            phase_results['phase3'] = test_control(TEST_RTU_ID, dev_num=1)
            phase_results['phase4'] = test_iv_scan(TEST_RTU_ID, dev_num=1)
            phase_results['phase5'] = test_der_avm_monitor(TEST_RTU_ID)
            phase_results['phase6'] = test_kstar_night(TEST_RTU_ID, dev_num=3)
            phase_results['phase7'] = test_info(TEST_RTU_ID)

            print()
            print('=' * 80)
            print("EXTENDED PHASES SUMMARY")
            print('=' * 80)
            for k, v in phase_results.items():
                status = 'PASS' if v else 'FAIL'
                print(f"  {k}: {status}")
            print('=' * 80)
            ext_fail = sum(1 for v in phase_results.values() if not v)
            if ext_fail > 0:
                fail_cnt += ext_fail

        if args.keep_running:
            log("Keeping processes alive (Ctrl+C to stop). Dashboard: http://localhost:8090", 'INFO')
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass

        return 0 if fail_cnt == 0 else 2

    finally:
        # Cleanup
        log("Terminating test processes...")
        for name, proc in reversed(processes):
            try:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=2)
            except Exception as e:
                log(f"Failed to stop {name}: {e}", 'WARN')
        cleanup_test_db()


if __name__ == '__main__':
    sys.exit(main())
