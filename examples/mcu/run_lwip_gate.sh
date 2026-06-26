#!/usr/bin/env bash
# The reproducible live lwIP gate driver for the on-device Wi-Fi example.
#
# It cross-builds the Wi-Fi firmware for the esp32 target (the on-target compile proof of the lwIP
# transport + the example firmware), then loops N>=3 times. The device is the DIALER: it boots,
# joins the AP, takes a DHCP lease, then dials the fixed host endpoint. So each iteration starts the
# host TCP server FIRST (it listens/accepts), then flashes the board, drives the auto-reset lines
# into RUN, and captures the device serial log to a per-run file for diagnostics. The host server
# blocks until it receives the device's first real message or times out, and asserts it. The whole
# gate FAILS if ANY iteration misses the message — a single lucky pass never substantiates a link
# claim. On all-pass it prints the run tally and exits 0.
#
# The real SSID/password live in main/wifi_credentials.h (gitignored, operator-supplied); this
# script never reads, hardcodes, or echoes them. Create it from the .example before running. Run
# from anywhere — paths resolve from the script's own location.
#
# Usage: run_lwip_gate.sh [RUNS] [PORT] [HOST_PORT]
#   RUNS       number of flash+assert iterations (default 3, clamped to a minimum of 3)
#   PORT       the flash serial device the board enumerates as (default /dev/ttyUSB0)
#   HOST_PORT  the TCP port the host server listens on (default 7447, must match the firmware)

set -euo pipefail

RUNS="${1:-3}"
PORT="${2:-/dev/ttyUSB0}"
HOST_PORT="${3:-7447}"

if [[ "${RUNS}" -lt 3 ]]; then
    echo "run count ${RUNS} is below the minimum of 3 reproducible runs; clamping to 3"
    RUNS=3
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IDF_PROJECT="${REPO_ROOT}/examples/mcu/esp-idf-lwip"
HOST_GATE="${REPO_ROOT}/build/examples/mcu/lwip_gate_host"
CREDENTIALS="${IDF_PROJECT}/main/wifi_credentials.h"
LOG_DIR="${REPO_ROOT}/build/examples/mcu/lwip_gate_logs"

if [[ ! -x "${HOST_GATE}" ]]; then
    echo "host gate binary not found at ${HOST_GATE}"
    echo "build it first: cmake --build build -j4 --target lwip_gate_host"
    exit 1
fi

if [[ ! -f "${CREDENTIALS}" ]]; then
    echo "Wi-Fi credentials not found at ${CREDENTIALS}"
    echo "create it from the template (it is gitignored and never committed):"
    echo "  cp ${CREDENTIALS}.example ${CREDENTIALS}   # then fill in SSID/password"
    exit 1
fi

# Reset the board into RUN via the dev-board auto-reset circuit (EN/IO0 tied to the adapter's
# RTS/DTR): IO0 high (run, not the download ROM), then pulse EN low->high. The SAME open then
# captures the device console (115200) to a per-run log for the capture window, so a missed
# receipt is diagnosable (join / DHCP lease / dial) — one open means one controlled reset, no
# second port-open re-resetting the board mid-run. Run in the background; kill after the receipt.
reset_and_capture() {   # $1=port  $2=device-log  $3=capture-seconds
    # exec so the backgrounded job IS the python process (not a wrapping subshell), otherwise
    # killing the subshell orphans python and it keeps the serial port for its full window.
    exec python3 - "$1" "$2" "$3" <<'PY'
import sys, time, serial
port, devlog, dur = sys.argv[1], sys.argv[2], float(sys.argv[3])
s = serial.Serial(port, 115200, timeout=0.2)
s.dtr = False   # IO0 high -> run the app
s.rts = True    # EN low   -> assert reset
time.sleep(0.1)
s.rts = False   # EN high  -> release -> boot + run
end = time.time() + dur
with open(devlog, "wb") as out:
    while time.time() < end:
        d = s.read(4096)
        if d:
            out.write(d); out.flush()
s.close()
PY
}

# shellcheck disable=SC1091
. /opt/esp-idf/export.sh

# The on-target compile: set the target once, then build. A non-zero exit fails the gate before a
# single flash — the cross-build IS the on-device compile proof of the lwIP transport + the example.
echo "=== cross-building Wi-Fi firmware for esp32 ==="
idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" set-target esp32
idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" build

mkdir -p "${LOG_DIR}"

passes=0
for ((run = 1; run <= RUNS; ++run)); do
    log="${LOG_DIR}/run_${run}.log"

    # Flash + reset FIRST, then start the host server. The device is the dialer, but its boot +
    # Wi-Fi association + DHCP after reset takes several seconds, whereas the server listens within
    # ~1s of launch — so starting it here still wins the race, AND the gate's receive deadline now
    # covers only the device's boot+join+dial, not the ~20s flash (which would otherwise consume the
    # window and time the gate out before the board ever dials).
    echo "=== run ${run}/${RUNS}: flashing ${PORT} ==="
    idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" -p "${PORT}" flash >>"${log}" 2>&1

    devlog="${LOG_DIR}/run_${run}_device.log"
    reset_and_capture "${PORT}" "${devlog}" 75 &
    cap_pid=$!

    echo "=== run ${run}/${RUNS}: starting host server on tcp 0.0.0.0:${HOST_PORT} ==="
    "${HOST_GATE}" "${HOST_PORT}" >"${log}" 2>&1 &
    gate_pid=$!

    echo "=== run ${run}/${RUNS}: awaiting receipt (device dials, host accepts) ==="
    if wait "${gate_pid}"; then
        passes=$((passes + 1))
        kill "${cap_pid}" 2>/dev/null || true
        wait "${cap_pid}" 2>/dev/null || true
        # The capture held the serial port; let the USB tty fully release before the next flash,
        # else esptool races it and fails with "multiple access on port".
        sleep 3
    else
        kill "${cap_pid}" 2>/dev/null || true
        wait "${cap_pid}" 2>/dev/null || true
        echo "GATE: run ${run}/${RUNS} did NOT receive the message (host: ${log}; device serial: ${devlog})"
        echo "GATE: ${passes}/${RUNS} runs received the message — FAIL"
        exit 1
    fi
done

echo "GATE: ${passes}/${RUNS} runs received the message"
exit 0
