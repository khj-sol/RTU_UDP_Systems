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


def validate_devices(data, expected):
    """Validate device data against expected MPPT/String counts.

    expected = {'INV_1': ('solarize', 4, 8), ...}
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
            freq = d.get('frequency', 0)
            proto, exp_m, exp_s = expected.get(key, ('?', 0, 0))
            issues = []
            if len(mppt) != exp_m:
                issues.append(f"mppt={len(mppt)}/{exp_m}")
            if len(strings) != exp_s:
                issues.append(f"str={len(strings)}/{exp_s}")
            if ac <= 0:
                issues.append(f"ac={ac}W")
            if freq < 50 or freq > 65:
                issues.append(f"freq={freq}")
            # MPPT voltage sanity (API returns V already, expect ≥100V when ac>0)
            for i, m in enumerate(mppt):
                v = m.get('voltage', 0)
                if v < 100 and ac > 0:
                    issues.append(f"MPPT{i+1}_V={v}")
                    break  # one is enough
            status = 'OK' if not issues else 'FAIL'
            results.append((key, proto, status, issues, ac, len(mppt), len(strings)))
        elif dtype in ('RELAY', 'WEATHER'):
            results.append((key, dtype.lower(), 'OK', [], 0, 0, 0))
    return results


def main():
    parser = argparse.ArgumentParser(description='Integration test runner')
    parser.add_argument('--wait', type=int, default=20,
                        help='Wait seconds after startup before API check (default: 20)')
    parser.add_argument('--keep-running', action='store_true',
                        help='Keep processes running after test (manual inspection)')
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

        # 6. Validate
        expected = {
            'INV_1': ('solarize', 4, 8),
            'INV_2': ('sungrow', 4, 8),
            'INV_3': ('kstar', 3, 9),
            'INV_4': ('huawei', 4, 0),
            'INV_5': ('ekos', 1, 2),
            'INV_6': ('senergy', 4, 8),
            'INV_7': ('sofar', 4, 4),
            'INV_8': ('solis', 4, 0),
            'INV_9': ('growatt', 2, 8),
            'INV_10': ('cps', 4, 0),
            'INV_11': ('sunways', 3, 6),
        }
        results = validate_devices(data, expected)

        print()
        print('=' * 80)
        print(f"{'Device':<10} {'Proto':<12} {'Status':<6} {'AC':<10} {'MPPT':<6} {'STR':<6}")
        print('=' * 80)
        pass_cnt = fail_cnt = 0
        for key, proto, status, issues, ac, m, s in results:
            lvl = 'OK' if status == 'OK' else 'FAIL'
            if status == 'OK':
                pass_cnt += 1
            else:
                fail_cnt += 1
            print(f"{key:<10} {proto:<12} {status:<6} {ac:<10} {m:<6} {s:<6}")
            if issues:
                print(f"           ISSUES: {', '.join(issues)}")
        print('=' * 80)
        print(f"RESULT: {pass_cnt} PASS / {fail_cnt} FAIL / {len(results)} TOTAL")
        print()

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
