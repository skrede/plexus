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

# The ESP32 dev-board auto-reset circuit ties EN/IO0 to the adapter's RTS/DTR; leaving those lines
# asserted holds the board reset-looping. Drive the classic auto-reset-into-RUN pulse on the flash
# port: IO0 high (run, not the download ROM), then pulse EN low->high. A host-link concern only.
reset_into_run() {
    python3 - "${PORT}" <<'PY'
import sys, termios, fcntl, time
fd = open(sys.argv[1], "rb+", buffering=0).fileno()
DTR, RTS = termios.TIOCM_DTR, termios.TIOCM_RTS
def clear(bit): fcntl.ioctl(fd, termios.TIOCMBIC, bit.to_bytes(4, "little"))
def set_(bit):  fcntl.ioctl(fd, termios.TIOCMBIS, bit.to_bytes(4, "little"))
clear(DTR)   # IO0 high -> boot the application, not the download ROM
set_(RTS)    # EN low   -> assert reset
time.sleep(0.1)
clear(RTS)   # EN high  -> release -> the board boots and runs the app
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

    # The device is the dialer, so the host server must already be listening when the board finishes
    # booting + associating. Start it FIRST in the background, then flash + reset the board.
    echo "=== run ${run}/${RUNS}: starting host server on tcp 0.0.0.0:${HOST_PORT} ==="
    "${HOST_GATE}" "${HOST_PORT}" >"${log}" 2>&1 &
    gate_pid=$!

    echo "=== run ${run}/${RUNS}: flashing ${PORT} ==="
    idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" -p "${PORT}" flash >>"${log}" 2>&1
    reset_into_run

    echo "=== run ${run}/${RUNS}: awaiting receipt (device dials, host accepts) ==="
    if wait "${gate_pid}"; then
        passes=$((passes + 1))
    else
        echo "GATE: run ${run}/${RUNS} did NOT receive the message (see ${log})"
        echo "GATE: ${passes}/${RUNS} runs received the message — FAIL"
        exit 1
    fi
done

echo "GATE: ${passes}/${RUNS} runs received the message"
exit 0
