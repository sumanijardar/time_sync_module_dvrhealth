# -*- coding: utf-8 -*-
import requests
from requests.auth import HTTPDigestAuth
import mysql.connector
import time
from datetime import datetime, timedelta
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
SLEEP_MINUTES = 120   # 120 minutes = 2 hours
NTP_SERVER          = "192.168.100.24"  # NTP server

FMT = "%Y-%m-%d %H:%M:%S"

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
# DATABASE - Load CP Plus (Dahua) Devices
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
            "WHERE dvrname IN ('CPPLUS', 'CP PLUS', 'DAHUVA') AND live = 'Y' "
            "AND IPAddress IS NOT NULL AND IPAddress != '';"
        )


        # query = (
        #     "SELECT ATMID as atm_id, IPAddress as ip, port, "
        #     "UserName as username, Password as password "
        #     "FROM all_dvr_live "
        #     "WHERE dvrname IN ('CPPLUS', 'CP PLUS', 'DAHUVA') "
        #     "AND live = 'Y' "
        #     "AND IPAddress IN ('172.17.19.155');"
        # )


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
        log_ok(f"Loaded {len(devices)} CP Plus & DAHUVA devices from database.")
    except Exception as e:
        log_err(f"DB Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
    return devices

# ============================================================
# CP Plus (DAHUVA) API - GET Device Time
# ============================================================
def get_cpplus_time(ip, port, username, password):
    """Fetch current time from CP Plus / DAHUVA DVR via CGI API."""
    auth = HTTPDigestAuth(username, password)
    url = f"http://{ip}:{port}/cgi-bin/global.cgi?action=getCurrentTime"
    try:
        res = requests.get(url, auth=auth, timeout=5)
        if res.status_code == 200:
            # Response format: result=2026-09-09 11:30:00
            device_time_str = res.text.strip().split("=")[-1]
            device_time = datetime.strptime(device_time_str, FMT)
            return device_time, device_time_str
    except Exception:
        pass
    raise Exception("Failed to fetch time from CP Plus & DAHUVA DVR")

# ============================================================
# CP Plus (DAHUVA) API - SET Time Manually
# ============================================================
def set_cpplus_time(ip, port, username, password, new_time):
    """Manually set time on CP Plus / DAHUVA DVR via CGI API (optional)."""
    auth = HTTPDigestAuth(username, password)
    time_str = new_time.strftime(FMT)
    url = f"http://{ip}:{port}/cgi-bin/global.cgi?action=setCurrentTime&time={time_str}"
    try:
        res = requests.get(url, auth=auth, timeout=5)
        if res.status_code == 200 and "OK" in res.text.upper():
            info(f"  Successfully set manual time to {time_str}")
            return True
    except Exception as e:
        pass
    log_warn(f"Manual time set not supported, skipping...")
    return False

# ============================================================
# CP Plus (DAHUVA) API - Configure NTP
# ============================================================
def configure_cpplus_ntp(ip, port, username, password):
    """Enable NTP, set NTP server address, and set update interval to 1 min on CP Plus / Dahua DVR."""
    auth = HTTPDigestAuth(username, password)
    ntp_url = (
        f"http://{ip}:{port}/cgi-bin/configManager.cgi?action=setConfig"
        f"&NTP.Address={NTP_SERVER}&NTP.Enable=true&NTP.UpdatePeriod=1"
    )
    try:
        res = requests.get(ntp_url, auth=auth, timeout=5)
        if res.status_code == 200 and "OK" in res.text.upper():
            info(f"  NTP configured -> server: {NTP_SERVER} (period: 1 min)")
            return True
    except Exception as e:
        info(f"  Failed to configure NTP: {e}")
    raise Exception("Failed to configure NTP on CP Plus & DAHUVA DVR")

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
        log_ok(f"DB updated: sync flag set for IP {ip}")
    except Exception as e:
        log_err(f"DB Update Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# ============================================================
# PROCESS ONE DEVICE
# ============================================================
def process_device(dev, index, total):
    ip       = dev["ip"]
    port     = dev["port"]
    username = dev["username"]
    password = dev["password"]
    atm_id   = dev["atm_id"]

    print()
    info(f"[{index}/{total}]  ATM: {atm_id}   IP: {ip}:{port}")

    try:
        device_time, raw_str = get_cpplus_time(ip, port, username, password)
        local_time  = datetime.now()
        delta       = abs((local_time - device_time).total_seconds())

        info(f"  Local Time  : {local_time.strftime(FMT)}")
        info(f"  Device Time : {device_time.strftime(FMT)}")
        info(f"  Difference  : {int(delta)} seconds")

        if delta > TIME_DIFF_THRESHOLD:
            # 1. Try manual time set
            set_cpplus_time(ip, port, username, password, local_time)
            # 2. Configure NTP
            configure_cpplus_ntp(ip, port, username, password)

            # 3. Re-check DVR time to verify if it actually changed
            time.sleep(2)
            try:
                new_dev_time, _ = get_cpplus_time(ip, port, username, password)
                new_delta = abs((datetime.now() - new_dev_time).total_seconds())
                if new_delta <= TIME_DIFF_THRESHOLD:
                    log_warn(f"Time SYNCED! diff was {int(delta)}s -> Now: {int(new_delta)}s (Fixed)")
                    update_sync_flag_in_db(ip)
                    return "synced", new_delta
                else:
                    log_warn(f"NTP configured, but DVR clock not synced yet (diff: {int(new_delta)}s). DB flag NOT updated.")
                    return "pending", new_delta
            except Exception:
                log_warn("NTP configured, but unable to re-verify time. DB flag NOT updated.")
                return "pending", delta
        else:
            log_ok(f"Time OK -- diff is only {int(delta)}s (threshold: {TIME_DIFF_THRESHOLD}s)")
            return "ok", delta

    except Exception as e:
        log_err(f"Error: {e}")
        return "failed", None


# ============================================================
# MAIN LOOP
# ============================================================
def main_loop():
    cycle = 0
    while True:
        cycle += 1
        now = datetime.now().strftime(FMT)

        header(f"CP Plus Time Sync  |  Cycle #{cycle}  |  {now}")

        section("Loading CP Plus & DAHUVA Devices from Database...")
        devices = load_devices_from_db()

        if not devices:
            log_err("No CP Plus & DAHUVA devices found. Waiting for next cycle...")
        else:
            section(f"Time Check and Sync  |  Total: {len(devices)} devices")
            total      = len(devices)
            synced     = 0
            already_ok = 0
            pending    = 0
            failed     = 0

            for i, dev in enumerate(devices, start=1):
                result, delta = process_device(dev, i, total)
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
            log_ok(f"Time OK  (no change needed) : {already_ok}")
            log_warn(f"Time Synced  (corrected)    : {synced}")
            if pending > 0:
                log_warn(f"NTP Configured (Sync Pending): {pending}")
            log_err(f"Failed / Unreachable         : {failed}")

        next_run = (datetime.now() + timedelta(minutes=SLEEP_MINUTES)).strftime(FMT)
        print()
        info(f"Next cycle in {SLEEP_MINUTES} minutes at {next_run}")
        divider("=")
        time.sleep(SLEEP_MINUTES * 60)


if __name__ == "__main__":
    main_loop()
