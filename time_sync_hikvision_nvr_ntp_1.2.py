# -*- coding: utf-8 -*-
import requests
from requests.auth import HTTPDigestAuth
import mysql.connector
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from colorama import init, Fore, Style

init(autoreset=True)

# ============================================================
# CONFIG
# ============================================================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "esurv",
}

TIME_DIFF_THRESHOLD = 60    # seconds threshold for time drift sync
SLEEP_MINUTES       = 120   # 120 minutes = 2 hours interval
MAX_WORKERS         = 30    # Parallel thread pool workers
NTP_SERVER          = "192.168.100.24"  # NTP server IP
TIMEZONE_SUFFIX     = "+05:30"
TIMEZONE_NAME       = "CST-5:30:00"

FMT = "%Y-%m-%d %H:%M:%S"

print_lock = threading.Lock()

# ============================================================
# CONSOLE HELPERS
# ============================================================
def divider(char="-", n=65):
    print(Fore.WHITE + Style.DIM + char * n)

def header(msg):
    divider("=")
    print(Fore.CYAN + Style.BRIGHT + "  " + msg)
    divider("=")

def info(msg):
    print(Fore.WHITE + "      " + msg)

def log_ok(msg):
    print(Fore.GREEN + "  [OK]  " + msg)

def log_warn(msg):
    print(Fore.YELLOW + "  [!!]  " + msg)

def log_err(msg):
    print(Fore.RED + "  [XX]  " + msg)

def section(msg):
    divider()
    print(Fore.CYAN + "  SECTION: " + msg)
    divider()

# ============================================================
# DATABASE - Load Hikvision NVR Devices
# ============================================================
def load_devices_from_db():
    """Fetch active Hikvision NVR devices from database."""
    devices = []
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = (
            "SELECT ATMID as atm_id, IPAddress as ip, port, "
            "UserName as username, Password as password "
            "FROM all_dvr_live "
            "WHERE dvrname IN ('Hikvision_nvr', 'Hikvision_NVR', 'HIK_NVR', 'HIKVISION_NVR') AND live = 'Y' "
            "AND IPAddress IS NOT NULL AND IPAddress != '';"
        )

        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            devices.append({
                "atm_id":   str(row.get("atm_id")   or "").strip(),
                "ip":       str(row.get("ip")        or "").strip(),
                "port":     int(row.get("port")      or 80),
                "username": str(row.get("username")  or "admin").strip(),
                "password": str(row.get("password")  or "").strip(),
            })
        log_ok(f"Loaded {len(devices)} Hikvision NVR devices from database.")
    except Exception as e:
        log_err(f"DB Error while loading Hikvision NVR devices: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
    return devices

# ============================================================
# HIKVISION NVR API - GET NVR Time
# ============================================================
def get_nvr_time(ip, port, username, password):
    """Fetch current time from Hikvision NVR via ISAPI."""
    url = f"http://{ip}:{port}/ISAPI/System/time"
    auth = HTTPDigestAuth(username, password)
    res = requests.get(url, auth=auth, timeout=5)
    res.raise_for_status()
    if "<localTime>" in res.text:
        time_str = res.text.split("<localTime>")[1].split("</localTime>")[0].split("+")[0]
        device_time = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
        return device_time, time_str
    raise ValueError("localTime not found in response")

# ============================================================
# HIKVISION NVR API - SET Manual Time
# ============================================================
def set_manual_time(ip, port, username, password, current_time):
    """Set time to manual mode with current system time on Hikvision NVR."""
    xml = f"""<?xml version="1.0" encoding="UTF-8" ?>
<Time version="1.0" xmlns="http://www.std-cgi.com/ver20/XMLSchema">
    <timeMode>manual</timeMode>
    <localTime>{current_time.strftime('%Y-%m-%dT%H:%M:%S')}{TIMEZONE_SUFFIX}</localTime>
    <timeZone>{TIMEZONE_NAME}</timeZone>
</Time>"""
    url = f"http://{ip}:{port}/ISAPI/System/time"
    headers = {'Content-Type': 'application/xml'}
    auth = HTTPDigestAuth(username, password)
    r = requests.put(url, data=xml.encode('utf-8'), headers=headers, auth=auth, timeout=5)
    r.raise_for_status()
    return True

# ============================================================
# HIKVISION NVR API - Switch to NTP Mode & Configure Server
# ============================================================
def switch_to_ntp(ip, port, username, password, current_time):
    """Switch Hikvision NVR time mode to NTP."""
    xml = f"""<?xml version="1.0" encoding="UTF-8" ?>
<Time version="1.0" xmlns="http://www.std-cgi.com/ver20/XMLSchema">
    <timeMode>NTP</timeMode>
    <localTime>{current_time.strftime('%Y-%m-%dT%H:%M:%S')}{TIMEZONE_SUFFIX}</localTime>
    <timeZone>{TIMEZONE_NAME}</timeZone>
</Time>"""
    url = f"http://{ip}:{port}/ISAPI/System/time"
    headers = {'Content-Type': 'application/xml'}
    auth = HTTPDigestAuth(username, password)
    r = requests.put(url, data=xml.encode('utf-8'), headers=headers, auth=auth, timeout=5)
    r.raise_for_status()
    return True

def configure_ntp_server(ip, port, username, password):
    """Configure NTP Server address on Hikvision NVR."""
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<NTPServer version="1.0" xmlns="http://www.std-cgi.com/ver20/XMLSchema">
    <id>1</id>
    <addressingFormatType>ipaddress</addressingFormatType>
    <ipAddress>{NTP_SERVER}</ipAddress>
    <portNo>123</portNo>
    <synchronizeInterval>60</synchronizeInterval>
</NTPServer>"""
    url = f"http://{ip}:{port}/ISAPI/System/time/ntpServers/1"
    headers = {'Content-Type': 'application/xml'}
    auth = HTTPDigestAuth(username, password)
    try:
        r = requests.put(url, data=xml.encode('utf-8'), headers=headers, auth=auth, timeout=5)
        if r.status_code in [200, 204]:
            return True
    except Exception:
        pass
    return False

# ============================================================
# DATABASE - Update sync flag after time correction
# ============================================================
def update_sync_flag_in_db(ip):
    """
    After syncing Hikvision NVR time, update all_dvr_live:
      dvrtime_sync_flag = '1'
      dvrtime_udate = current datetime
    """
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        now_str = datetime.now().strftime(FMT)
        update_query = (
            "UPDATE all_dvr_live SET dvrtime_sync_flag = '1', "
            "dvrtime_udate = %s WHERE IPAddress = %s"
        )
        cursor.execute(update_query, (now_str, ip))
        conn.commit()
        return True
    except Exception as e:
        with print_lock:
            log_err(f"DB Update Error for {ip}: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# ============================================================
# PROCESS ONE NVR DEVICE (Thread Safe - Concise Output)
# ============================================================
def process_device(dev, index, total):
    ip       = dev["ip"]
    port     = dev["port"]
    username = dev["username"]
    password = dev["password"]
    atm_id   = dev["atm_id"]

    try:
        nvr_time, raw_str = get_nvr_time(ip, port, username, password)
        system_time = datetime.now().replace(microsecond=0)
        delta = abs((system_time - nvr_time).total_seconds())

        if delta > TIME_DIFF_THRESHOLD:
            # Step 1: Set manual time
            set_manual_time(ip, port, username, password, system_time)
            # Step 2: Configure NTP server address
            configure_ntp_server(ip, port, username, password)
            # Step 3: Switch to NTP mode
            switch_to_ntp(ip, port, username, password, system_time)

            # Re-check NVR time to verify correction
            time.sleep(2)
            try:
                new_nvr_time, _ = get_nvr_time(ip, port, username, password)
                new_delta = abs((datetime.now().replace(microsecond=0) - new_nvr_time).total_seconds())
                if new_delta <= TIME_DIFF_THRESHOLD:
                    update_sync_flag_in_db(ip)
                    with print_lock:
                        log_warn(f"{ip}:{port} (ATM: {atm_id}) - Time SYNCED! diff: {int(delta)}s -> {int(new_delta)}s (Fixed & DB Updated)")
                    return "synced", new_delta
                else:
                    with print_lock:
                        log_warn(f"{ip}:{port} (ATM: {atm_id}) - NTP Configured, sync pending (diff: {int(new_delta)}s)")
                    return "pending", new_delta
            except Exception:
                with print_lock:
                    log_warn(f"{ip}:{port} (ATM: {atm_id}) - NTP Configured, unable to re-verify")
                return "pending", delta
        else:
            with print_lock:
                log_ok(f"{ip}:{port} (ATM: {atm_id}) - Time OK ({int(delta)}s diff)")
            return "ok", delta

    except Exception as e:
        with print_lock:
            log_err(f"{ip}:{port} (ATM: {atm_id}) - Error: {e}")
        return "failed", None

# ============================================================
# MAIN LOOP (Thread Pool)
# ============================================================
def main_loop():
    cycle = 0
    while True:
        cycle += 1
        now = datetime.now().strftime(FMT)

        header(f"Hikvision NVR Time Sync 1.2  |  Cycle #{cycle}  |  {now}")

        devices = load_devices_from_db()

        if not devices:
            log_err("No Hikvision NVR devices found. Waiting for next cycle...")
        else:
            section(f"Time Check and Sync ({MAX_WORKERS} Parallel Threads)  |  Total: {len(devices)} devices")
            total      = len(devices)
            synced     = 0
            already_ok = 0
            pending    = 0
            failed     = 0

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_device, dev, i, total)
                    for i, dev in enumerate(devices, start=1)
                ]
                for future in as_completed(futures):
                    result, delta = future.result()
                    if result == "synced":
                        synced += 1
                    elif result == "ok":
                        already_ok += 1
                    elif result == "pending":
                        pending += 1
                    else:
                        failed += 1

            print()
            section("Cycle Summary")
            log_ok(f"Time OK  (no change needed)  : {already_ok}")
            log_warn(f"Time Synced  (corrected)     : {synced}")
            if pending > 0:
                log_warn(f"NTP Configured (Sync Pending): {pending}")
            log_err(f"Failed / Unreachable          : {failed}")

        next_run = (datetime.now() + timedelta(minutes=SLEEP_MINUTES)).strftime(FMT)
        print()
        info(f"Next cycle in {SLEEP_MINUTES} minutes at {next_run}")
        divider("=")
        time.sleep(SLEEP_MINUTES * 60)

if __name__ == "__main__":
    main_loop()
