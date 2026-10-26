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

TIME_DIFF_THRESHOLD = 180   # seconds - sync if diff > this
SLEEP_MINUTES       = 120   # 120 minutes = 2 hours
MAX_WORKERS         = 30    # Number of parallel threads

HEADERS = {"Content-Type": "application/json"}

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
# DATABASE - Load UNV Devices
# ============================================================
def load_devices_from_db():
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
            "WHERE dvrname IN ('UNV', 'UNIVIEW') AND live = 'Y' "
            "AND IPAddress IS NOT NULL AND IPAddress != '';"
        )
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            devices.append({
                "atm_id":   str(row.get("atm_id")   or "").strip(),
                "ip":       str(row.get("ip")        or "").strip(),
                "port":     int(row.get("port")      or 81),
                "username": str(row.get("username")  or "admin").strip(),
                "password": str(row.get("password")  or "").strip(),
            })
        log_ok(f"Loaded {len(devices)} UNV devices from database.")
    except Exception as e:
        log_err(f"DB Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
    return devices

# ============================================================
# UNV API - GET Time
# ============================================================
def get_unv_time(ip, port, username, password):
    auth = HTTPDigestAuth(username, password)
    # Try /System/Time first, fallback to /System/TimeNTP
    for ep in ["/LAPI/V1.0/System/Time", "/LAPI/V1.0/System/TimeNTP"]:
        try:
            url = f"http://{ip}:{port}{ep}"
            res = requests.get(url, auth=auth, timeout=5)
            if res.status_code == 200:
                data = res.json()
                ts = data["Response"]["Data"]["DeviceTime"]
                return datetime.fromtimestamp(ts), data["Response"]["Data"]
        except Exception:
            continue
    raise Exception("Failed to fetch time from UNV DVR")

# ============================================================
# UNV API - SET Time  (Read -> Modify -> Write pattern)
# ============================================================
def set_unv_time(ip, port, username, password, new_time):
    """
    Uniview LAPI — Read -> Modify -> Write pattern:
      Primary: GET/PUT to /LAPI/V1.0/System/Time (directly sets DeviceTime)
      Fallback: GET/PUT to /LAPI/V1.0/System/TimeNTP
    """
    auth = HTTPDigestAuth(username, password)
    ts   = int(new_time.timestamp())

    for ep in ["/LAPI/V1.0/System/Time", "/LAPI/V1.0/System/TimeNTP"]:
        url = f"http://{ip}:{port}{ep}"
        try:
            get_res = requests.get(url, auth=auth, timeout=5)
            if get_res.status_code != 200:
                continue
            
            current_config = get_res.json()["Response"]["Data"]
            current_config["DeviceTime"] = ts

            put_res = requests.put(url, auth=auth, headers=HEADERS, json=current_config, timeout=5)
            if put_res.status_code in [200, 204]:
                put_data = put_res.json()
                code = put_data.get("Response", {}).get("ResponseCode", -1)
                if code == 0:
                    return True
        except Exception:
            continue

    raise Exception("Failed to update time on UNV DVR via available endpoints")

# ============================================================
# DATABASE - Update sync flag after time correction
# ============================================================
def update_sync_flag_in_db(ip):
    """
    After syncing DVR time, update all_dvr_live:
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
# PROCESS ONE DEVICE (Thread Safe)
# ============================================================
def process_device(dev, index, total):
    ip       = dev["ip"]
    port     = dev["port"]
    username = dev["username"]
    password = dev["password"]
    atm_id   = dev["atm_id"]

    try:
        device_time, ntp_data = get_unv_time(ip, port, username, password)
        local_time  = datetime.now()
        delta       = abs((local_time - device_time).total_seconds())

        if delta > TIME_DIFF_THRESHOLD:
            set_unv_time(ip, port, username, password, local_time)
            update_sync_flag_in_db(ip)
            with print_lock:
                print()
                info(f"[{index}/{total}]  ATM: {atm_id}   IP: {ip}:{port}")
                info(f"  Local Time  : {local_time.strftime(FMT)}")
                info(f"  Device Time : {device_time.strftime(FMT)}")
                info(f"  Difference  : {int(delta)} seconds")
                log_warn(f"Time SYNCED! diff was {int(delta)}s -> Fixed")
                log_ok(f"DB updated: sync flag set for IP {ip}")
            return "synced", delta
        else:
            with print_lock:
                print()
                info(f"[{index}/{total}]  ATM: {atm_id}   IP: {ip}:{port}")
                info(f"  Local Time  : {local_time.strftime(FMT)}")
                info(f"  Device Time : {device_time.strftime(FMT)}")
                info(f"  Difference  : {int(delta)} seconds")
                log_ok(f"Time OK -- diff is only {int(delta)}s (threshold: {TIME_DIFF_THRESHOLD}s)")
            return "ok", delta

    except Exception as e:
        with print_lock:
            print()
            info(f"[{index}/{total}]  ATM: {atm_id}   IP: {ip}:{port}")
            log_err(f"Error: {e}")
        return "failed", None


# ============================================================
# MAIN LOOP (Thread Pool)
# ============================================================
def main_loop():
    cycle = 0
    while True:
        cycle += 1
        now = datetime.now().strftime(FMT)

        header(f"UNV Time Sync  |  Cycle #{cycle}  |  {now}")

        section("Loading UNV Devices from Database...")
        devices = load_devices_from_db()

        if not devices:
            log_err("No UNV devices found. Waiting for next cycle...")
        else:
            section(f"Time Check and Sync (Parallel: {MAX_WORKERS} Threads)  |  Total: {len(devices)} devices")
            total      = len(devices)
            synced     = 0
            already_ok = 0
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
                    else:
                        failed += 1

            print()
            section("Cycle Summary")
            log_ok(f"Time OK  (no change needed)  : {already_ok}")
            log_warn(f"Time Synced  (corrected)     : {synced}")
            log_err(f"Failed / Unreachable          : {failed}")

        next_run = (datetime.now() + timedelta(minutes=SLEEP_MINUTES)).strftime(FMT)
        print()
        info(f"Next cycle in {SLEEP_MINUTES} minutes at {next_run}")
        divider("=")
        time.sleep(SLEEP_MINUTES * 60)


if __name__ == "__main__":
    main_loop()
