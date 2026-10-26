# -*- coding: utf-8 -*-
import requests
from requests.auth import HTTPDigestAuth
import xml.etree.ElementTree as ET
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

TIME_DIFF_THRESHOLD = 180   # seconds - sync if diff > threshold
SLEEP_MINUTES       = 120   # 120 minutes = 2 hours interval
MAX_WORKERS         = 30    # Parallel thread pool workers
NTP_SERVER          = "192.168.100.24"  # NTP server IP
TIMEZONE_SUFFIX     = "+05:30"
TIMEZONE_NAME       = "CST-5:30:00"

FMT = "%Y-%m-%d %H:%M:%S"

print_lock = threading.Lock()

# ============================================================
# CONSOLE HELPERS (PRODUCTION LOGGING)
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
# DATABASE - Load Hikvision Devices
# ============================================================
def load_devices_from_db():
    """Fetch active Hikvision DVR/NVR devices from database."""
    devices = []
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        # query = (
        #     "SELECT ATMID as atm_id, IPAddress as ip, port, "
        #     "UserName as username, Password as password "
        #     "FROM all_dvr_live "
        #     "WHERE dvrname IN ('hikvision', 'HIKVISION', 'HIK VISION') AND live = 'Y' "
        #     "AND IPAddress IS NOT NULL AND IPAddress != '';"
        # )


        query = (
            "SELECT ATMID as atm_id, IPAddress as ip, port, "
            "UserName as username, Password as password "
            "FROM all_dvr_live "
            "WHERE dvrname IN ('hikvision', 'HIKVISION', 'HIK VISION')"
            "AND live = 'Y' "
            "AND IPAddress IN ('172.17.8.118','172.17.7.129');"
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
        log_ok(f"Loaded {len(devices)} Hikvision devices from database.")
    except Exception as e:
        log_err(f"DB Error while loading Hikvision devices: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
    return devices

# ============================================================
# HIKVISION ISAPI - GET Device Time
# ============================================================
def get_hik_time(ip, port, username, password):
    """Fetch current local time from Hikvision DVR via ISAPI."""
    auth = HTTPDigestAuth(username, password)
    url = f"http://{ip}:{port}/ISAPI/System/time"
    try:
        res = requests.get(url, auth=auth, timeout=5)
        if res.status_code == 200:
            content = res.text
            # Try XML ElementTree parsing
            try:
                root = ET.fromstring(res.content)
                ns = {"ns": "http://www.hikvision.com/ver20/XMLSchema"}
                time_str = root.findtext("ns:localTime", namespaces=ns)
                if not time_str:
                    time_str = root.findtext("localTime")
                if time_str:
                    clean_str = time_str.split("+")[0].split(".")[0]
                    clean_str = clean_str.replace("T", " ")
                    device_time = datetime.strptime(clean_str, FMT)
                    return device_time, clean_str
            except Exception:
                pass

            # Fallback text parsing
            if "<localTime>" in content:
                time_str = content.split("<localTime>")[1].split("</localTime>")[0]
                clean_str = time_str.split("+")[0].split(".")[0].replace("T", " ")
                device_time = datetime.strptime(clean_str, FMT)
                return device_time, clean_str
    except Exception:
        pass
    raise Exception("Failed to fetch time from Hikvision DVR")

# ============================================================
# HIKVISION ISAPI - SET Time Manually
# ============================================================
def set_hik_manual_time(ip, port, username, password, new_time):
    """Set manual system time on Hikvision DVR via ISAPI PUT."""
    auth = HTTPDigestAuth(username, password)
    url = f"http://{ip}:{port}/ISAPI/System/time"
    time_iso = new_time.strftime("%Y-%m-%dT%H:%M:%S")
    xml_payload = f"""<?xml version="1.0" encoding="UTF-8"?>
<Time xmlns="http://www.hikvision.com/ver20/XMLSchema">
    <timeMode>manual</timeMode>
    <localTime>{time_iso}{TIMEZONE_SUFFIX}</localTime>
    <timeZone>{TIMEZONE_NAME}</timeZone>
</Time>"""
    headers = {'Content-Type': 'application/xml'}
    try:
        res = requests.put(url, data=xml_payload.encode('utf-8'), headers=headers, auth=auth, timeout=5)
        if res.status_code in [200, 204] or "statusCode" in res.text:
            return True
    except Exception:
        pass
    return False

# ============================================================
# HIKVISION ISAPI - Configure & Switch to NTP Mode
# ============================================================
def set_hik_ntp_mode(ip, port, username, password):
    """Enable NTP mode on Hikvision DVR via ISAPI PUT."""
    auth = HTTPDigestAuth(username, password)
    url = f"http://{ip}:{port}/ISAPI/System/time"
    xml_payload = f"""<?xml version="1.0" encoding="UTF-8"?>
<Time xmlns="http://www.hikvision.com/ver20/XMLSchema">
    <timeMode>NTP</timeMode>
    <timeZone>{TIMEZONE_NAME}</timeZone>
</Time>"""
    headers = {'Content-Type': 'application/xml'}
    try:
        res = requests.put(url, data=xml_payload.encode('utf-8'), headers=headers, auth=auth, timeout=5)
        if res.status_code in [200, 204] or "statusCode" in res.text:
            return True
    except Exception:
        pass
    return False

def configure_hik_ntp_server(ip, port, username, password):
    """Configure NTP server address and interval on Hikvision DVR."""
    auth = HTTPDigestAuth(username, password)
    url = f"http://{ip}:{port}/ISAPI/System/time/ntpServers/1"
    xml_payload = f"""<?xml version="1.0" encoding="UTF-8"?>
<NTPServer xmlns="http://www.hikvision.com/ver20/XMLSchema">
    <id>1</id>
    <addressingFormatType>ipaddress</addressingFormatType>
    <ipAddress>{NTP_SERVER}</ipAddress>
    <portNo>123</portNo>
    <synchronizeInterval>60</synchronizeInterval>
</NTPServer>"""
    headers = {'Content-Type': 'application/xml'}
    try:
        res = requests.put(url, data=xml_payload.encode('utf-8'), headers=headers, auth=auth, timeout=5)
        if res.status_code in [200, 204] or "statusCode" in res.text:
            return True
    except Exception:
        pass
    return False

# ============================================================
# DATABASE - Update sync flag after time correction
# ============================================================
def update_sync_flag_in_db(ip, dvr_time_str, time_diff):
    """
    After syncing Hikvision DVR time, update all_dvr_live:
      dvrtime_sync_flag = '1'
      dvrtime_udate = current datetime
    """
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        now_str = datetime.now().strftime(FMT)
        # update_query = (
        #     "UPDATE all_dvr_live SET dvrtime_sync_flag = '1', "
        #     "dvrtime_udate = %s WHERE IPAddress = %s"
        # )
        update_query = (
            "UPDATE all_dvr_live SET dvrtime_sync_flag = '1', "
            "dvrtime_udate = %s, dvr_time = %s, time_diff = %s "
            "WHERE IPAddress = %s"
        )
        # cursor.execute(update_query, (now_str, ip))
        cursor.execute(update_query, (now_str, dvr_time_str, time_diff, ip))
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
# PROCESS ONE DEVICE (Thread Safe - Production Console Output)
# ============================================================
def process_device(dev, index, total):
    ip       = dev["ip"]
    port     = dev["port"]
    username = dev["username"]
    password = dev["password"]
    atm_id   = dev["atm_id"]

    try:
        device_time, raw_str = get_hik_time(ip, port, username, password)
        local_time  = datetime.now()
        delta       = abs((local_time - device_time).total_seconds())

        if delta > TIME_DIFF_THRESHOLD:
            # 1. Manual time set
            manual_ok = set_hik_manual_time(ip, port, username, password, local_time)
            # 2. Configure NTP Server IP & Interval
            configure_hik_ntp_server(ip, port, username, password)
            # 3. Switch to NTP Mode
            ntp_mode_ok = set_hik_ntp_mode(ip, port, username, password)

            # 4. Re-check DVR time to verify correction
            time.sleep(2)
            try:
                new_dev_time, _ = get_hik_time(ip, port, username, password)
                new_delta = abs((datetime.now() - new_dev_time).total_seconds())
                if new_delta <= TIME_DIFF_THRESHOLD:
                    diff_formatted = str(timedelta(seconds=int(new_delta)))
                    update_sync_flag_in_db(ip, new_dev_time.strftime(FMT), diff_formatted)
                    with print_lock:
                        print()
                        info(f"[{index}/{total}]  ATM: {atm_id}   IP: {ip}:{port}")
                        info(f"  Local Time  : {local_time.strftime(FMT)}")
                        info(f"  Device Time : {device_time.strftime(FMT)}")
                        info(f"  Difference  : {int(delta)} seconds")
                        log_warn(f"Time SYNCED! diff was {int(delta)}s -> Now: {int(new_delta)}s (Fixed)")
                        log_ok(f"DB updated: sync flag set for IP {ip}")
                    return "synced", new_delta
                else:
                    with print_lock:
                        print()
                        info(f"[{index}/{total}]  ATM: {atm_id}   IP: {ip}:{port}")
                        info(f"  Local Time  : {local_time.strftime(FMT)}")
                        info(f"  Device Time : {device_time.strftime(FMT)}")
                        info(f"  Difference  : {int(delta)} seconds")
                        info(f"  NTP configured -> server: {NTP_SERVER} (Interval: 60m)")
                        log_warn(f"NTP configured, but Hikvision clock sync pending (diff: {int(new_delta)}s). DB flag NOT updated.")
                    return "pending", new_delta
            except Exception:
                with print_lock:
                    print()
                    info(f"[{index}/{total}]  ATM: {atm_id}   IP: {ip}:{port}")
                    info(f"  Local Time  : {local_time.strftime(FMT)}")
                    info(f"  Device Time : {device_time.strftime(FMT)}")
                    info(f"  Difference  : {int(delta)} seconds")
                    log_warn("NTP configured, but unable to re-verify time. DB flag NOT updated.")
                return "pending", delta
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

        header(f"Hikvision Time Sync 1.2  |  Cycle #{cycle}  |  {now}")

        section("Loading Hikvision Devices from Database...")
        devices = load_devices_from_db()

        if not devices:
            log_err("No Hikvision devices found. Waiting for next cycle...")
        else:
            section(f"Time Check and Sync (Parallel: {MAX_WORKERS} Threads)  |  Total: {len(devices)} devices")
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
