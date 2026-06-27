#!/usr/bin/env bash
# The reproducible bidirectional on-hardware IGMP gate driver for the on-device native-discovery
# example.
#
# It cross-builds the discovery firmware for the esp32 target (the on-target compile proof of the
# lwIP multicast leaf + the hoisted multicast_discovery template), then loops N>=3 times. The device
# boots, joins the AP, takes a DHCP lease, joins the multicast group, and both announces AND browses.
# Each iteration flashes the board (with the 5-attempt auto-reset retry), starts the host counterpart
# (which also announces and browses on the same group), drives the auto-reset lines into RUN, and
# captures the device serial to a per-run device log. After each run it greps the device log for
# GATE_PASS_HOST_TO_ESP (the device resolved the host) and the host log for GATE_PASS_ESP_TO_HOST
# (the host resolved the device), tallying passes per direction.
#
# A green cross-compile is explicitly NOT proof: ESP-lwIP arms the IGMP membership-report timer on
# demand, so only a live join + report on hardware proves the path. This script EMITS the raw device
# and host logs and prints a per-direction tally plus a single BIDIRECTIONAL verdict — it does NOT
# claim authority over the verdict; the raw logs are read on the main thread.
#
# The real SSID/password live in esp-idf-lwip-discovery/main/wifi_credentials.h (gitignored,
# operator-supplied); this script never reads, hardcodes, or echoes them. Create it from the
# .example before running. Set PLEXUS_HOST_ENDPOINT to the host's shared-subnet IP:port at run time.
#
# Usage: run_multicast_gate.sh [RUNS] [PORT]
#   RUNS  flash+capture iterations per direction (default 3, clamped to a minimum of 3)
#   PORT  the flash serial device the board enumerates as (default /dev/ttyUSB0)
#
# Environment:
#   PLEXUS_HOST_ENDPOINT  the host's shared-subnet IP:port (the host counterpart binds its listen
#                         port from the ':port' suffix; default 7447)

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
HOST_GATE="${REPO_ROOT}/build/examples/mcu/multicast_gate_host"
CREDENTIALS="${IDF_PROJECT}/main/wifi_credentials.h"
LOG_DIR="${REPO_ROOT}/build/examples/mcu/multicast_gate_logs"

HOST_ENDPOINT="${PLEXUS_HOST_ENDPOINT:-}"
HOST_PORT="${HOST_ENDPOINT##*:}"
if [[ -z "${HOST_PORT}" || "${HOST_PORT}" == "${HOST_ENDPOINT}" ]]; then
    HOST_PORT="7447"
fi

if [[ ! -x "${HOST_GATE}" ]]; then
    echo "host gate binary not found at ${HOST_GATE}"
    echo "build it first: cmake --build build -j4 --target multicast_gate_host"
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
# device console (115200) to a per-run log for the capture window, so the join / DHCP / resolve
# diagnostics land in one file. One open means one controlled reset, no second port-open re-resetting
# the board mid-run.
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
# single flash — the cross-build IS the on-device compile proof of the lwIP multicast leaf + the
# hoisted discovery template. It is NOT proof of the live join (the on-demand IGMP timer); that is
# what the per-run capture below substantiates.
echo "=== cross-building discovery firmware for esp32 ==="
idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" set-target esp32
idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" build

mkdir -p "${LOG_DIR}"

host_to_esp=0   # device resolved the host (GATE_PASS_HOST_TO_ESP in the device serial)
esp_to_host=0   # host resolved the device (GATE_PASS_ESP_TO_HOST in the host stdout)
declare -a DEVLOGS=()
declare -a HOSTLOGS=()

for ((run = 1; run <= RUNS; ++run)); do
    log="${LOG_DIR}/run_${run}_host.log"
    devlog="${LOG_DIR}/run_${run}_device.log"
    DEVLOGS+=("${devlog}")
    HOSTLOGS+=("${log}")

    echo "=== run ${run}/${RUNS}: flashing ${PORT} ==="
    # The dev-board auto-reset-into-bootloader (esptool DTR/RTS) is intermittently flaky on this
    # adapter ("chip stopped responding" ~1 in 3); retry the flash so one stuck reset does not abort
    # the sweep.
    flashed=0
    for attempt in 1 2 3 4 5; do
        if idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" -p "${PORT}" flash >>"${log}" 2>&1; then
            flashed=1; break
        fi
        echo "=== run ${run}/${RUNS}: flash attempt ${attempt} failed; settling then retrying ==="
        sleep 3
    done
    if [[ "${flashed}" -ne 1 ]]; then
        echo "=== run ${run}/${RUNS}: flash failed after retries (board not entering download mode) ==="
        sleep 3
        continue
    fi

    reset_and_capture "${PORT}" "${devlog}" 75 &
    cap_pid=$!

    echo "=== run ${run}/${RUNS}: starting host counterpart (announce + browse) on tcp 0.0.0.0:${HOST_PORT} ==="
    "${HOST_GATE}" "${HOST_PORT}" >"${log}" 2>&1 &
    host_pid=$!

    # Hold the capture window, then tear the host counterpart down. The device serial carries the
    # inbound resolve (GATE_PASS_HOST_TO_ESP); the host stdout carries GATE_PASS_ESP_TO_HOST.
    wait "${cap_pid}" 2>/dev/null || true
    kill "${host_pid}" 2>/dev/null || true
    wait "${host_pid}" 2>/dev/null || true

    if grep -q "GATE_PASS_HOST_TO_ESP" "${devlog}"; then
        host_to_esp=$((host_to_esp + 1))
        echo "=== run ${run}/${RUNS}: device resolved the host (GATE_PASS_HOST_TO_ESP) ==="
    else
        echo "=== run ${run}/${RUNS}: device did NOT resolve the host (device serial: ${devlog}) ==="
    fi
    if grep -q "GATE_PASS_ESP_TO_HOST" "${log}"; then
        esp_to_host=$((esp_to_host + 1))
        echo "=== run ${run}/${RUNS}: host resolved the device (GATE_PASS_ESP_TO_HOST) ==="
    else
        echo "=== run ${run}/${RUNS}: host did NOT resolve the device (host log: ${log}) ==="
    fi
    sleep 3
done

echo
echo "=== BIDIRECTIONAL IGMP GATE TALLY ==="
echo "host -> ESP32 (device resolved the host): ${host_to_esp}/${RUNS}"
echo "ESP32 -> host (host resolved the device): ${esp_to_host}/${RUNS}"
echo
echo "raw device logs:"
for f in "${DEVLOGS[@]}"; do echo "  ${f}"; done
echo "raw host logs:"
for f in "${HOSTLOGS[@]}"; do echo "  ${f}"; done
echo

if [[ "${host_to_esp}" -ge "${RUNS}" && "${esp_to_host}" -ge "${RUNS}" ]]; then
    echo "BIDIRECTIONAL GATE_PASS: both directions hit ${RUNS}/${RUNS} — verify against the raw logs above"
    exit 0
fi
echo "BIDIRECTIONAL GATE_FAIL: at least one direction missed the run count — read the raw logs above"
exit 1
