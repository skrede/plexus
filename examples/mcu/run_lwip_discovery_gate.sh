#!/usr/bin/env bash
# The reproducible on-hardware accept gate for the on-device honest-listen example.
#
# It cross-builds the discovery firmware for the esp32 target (the on-target compile proof of the
# lwIP acceptor leaf + the example firmware), then loops N>=3 times. The device is the SERVER: it
# boots, joins the AP, takes a DHCP lease, announces tcp:7447 on the multicast group, and binds the
# real lwIP acceptor. So each iteration flashes the device FIRST (with a 5-attempt auto-reset retry),
# drives the auto-reset lines into RUN, and captures the device serial to a per-run log; then it runs
# the host program, which discovers the device over multicast, DIALS the advertised tcp:7447,
# subscribes, and asserts an accepted session (the heartbeat flows). The whole gate FAILS if ANY
# iteration misses — a single lucky accept never substantiates the served-card claim. On all-pass it
# prints the run tally and exits 0.
#
# The real SSID/password live in main/wifi_credentials.h (gitignored, operator-supplied); this script
# never reads, hardcodes, or echoes them. Create it from the .example before running. Run from
# anywhere — paths resolve from the script's own location.
#
# Usage: run_lwip_discovery_gate.sh [RUNS] [PORT]
#   RUNS  number of flash+dial+assert iterations (default 3, clamped to a minimum of 3)
#   PORT  the flash serial device the board enumerates as (default /dev/ttyUSB0)

set -euo pipefail

RUNS="${1:-3}"
PORT="${2:-/dev/ttyUSB0}"

if [[ "${RUNS}" -lt 3 ]]; then
    echo "run count ${RUNS} is below the minimum of 3 reproducible runs; clamping to 3"
    RUNS=3
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IDF_PROJECT="${REPO_ROOT}/examples/mcu/esp-idf-lwip-discovery"
HOST_GATE="${REPO_ROOT}/build/examples/mcu/lwip_discovery_dial_host"
CREDENTIALS="${IDF_PROJECT}/main/wifi_credentials.h"
LOG_DIR="${REPO_ROOT}/build/examples/mcu/lwip_discovery_gate_logs"

if [[ ! -x "${HOST_GATE}" ]]; then
    echo "host gate binary not found at ${HOST_GATE}"
    echo "build it first: cmake --build build -j4 --target lwip_discovery_dial_host"
    exit 1
fi

if [[ ! -f "${CREDENTIALS}" ]]; then
    echo "Wi-Fi credentials not found at ${CREDENTIALS}"
    echo "create it from the template (it is gitignored and never committed):"
    echo "  cp ${CREDENTIALS}.example ${CREDENTIALS}   # then fill in SSID/password"
    exit 1
fi

# Reset the board into RUN via the dev-board auto-reset circuit (EN/IO0 tied to the adapter's
# RTS/DTR): IO0 high (run, not the download ROM), then pulse EN low->high. The SAME open captures the
# device console (115200) to a per-run log for the capture window, so the join / DHCP / accept
# diagnostics land in one file. One open means one controlled reset, no second port-open re-resetting
# the board mid-run. Run in the background; kill after the host gate returns.
reset_and_capture() {   # $1=port  $2=device-log  $3=capture-seconds
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
# single flash — the cross-build IS the on-device compile proof of the lwIP acceptor leaf + the
# example. It is NOT proof of a live accept; that is what the per-run dial below substantiates.
echo "=== cross-building discovery firmware for esp32 ==="
idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" set-target esp32
idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" build

mkdir -p "${LOG_DIR}"

passes=0
for ((run = 1; run <= RUNS; ++run)); do
    log="${LOG_DIR}/run_${run}_host.log"
    devlog="${LOG_DIR}/run_${run}_device.log"

    # Flash the device-as-server FIRST. The dev-board auto-reset-into-bootloader (esptool DTR/RTS) is
    # intermittently flaky on this adapter ("chip stopped responding" ~1 in 3); retry so one stuck
    # reset does not abort the sweep.
    echo "=== run ${run}/${RUNS}: flashing ${PORT} ==="
    flashed=0
    for attempt in 1 2 3 4 5; do
        if idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" -p "${PORT}" flash >>"${log}" 2>&1; then
            flashed=1; break
        fi
        echo "=== run ${run}/${RUNS}: flash attempt ${attempt} failed; settling then retrying ==="
        sleep 3
    done
    if [[ "${flashed}" -ne 1 ]]; then
        echo "GATE: run ${run}/${RUNS} could not flash the board (not entering download mode) — FAIL"
        exit 1
    fi

    reset_and_capture "${PORT}" "${devlog}" 75 &
    cap_pid=$!

    echo "=== run ${run}/${RUNS}: host discovers + dials the device's advertised tcp:7447 ==="
    if "${HOST_GATE}" >"${log}" 2>&1; then
        passes=$((passes + 1))
        echo "=== run ${run}/${RUNS}: host accepted session (host log: ${log}; device serial: ${devlog}) ==="
        kill "${cap_pid}" 2>/dev/null || true
        wait "${cap_pid}" 2>/dev/null || true
        # The capture held the serial port; let the USB tty fully release before the next flash, else
        # esptool races it and fails with "multiple access on port".
        sleep 3
    else
        kill "${cap_pid}" 2>/dev/null || true
        wait "${cap_pid}" 2>/dev/null || true
        echo "GATE: run ${run}/${RUNS} did NOT establish a session (host: ${log}; device serial: ${devlog})"
        echo "GATE: ${passes}/${RUNS} runs accepted — FAIL"
        exit 1
    fi
done

echo "GATE: ${passes}/${RUNS} runs accepted"
exit 0
