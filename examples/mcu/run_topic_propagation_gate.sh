#!/usr/bin/env bash
# The live gate for cross-participant topic propagation over the serial link. It cross-builds both firmwares for
# esp32, then loops N>=3 times, asserting two claims on the hardware in each iteration:
#
#   1. propagation: the board runs the topic-graph firmware and the host gate dials it; the host must enumerate the
#      device's topic with the type name the device declared, and the device must report the host's topic with its
#      type name. Both directions or fail.
#   2. direct-only enumeration: the board runs the peerless onboard firmware, whose participants() snapshot must read
#      count=0 truncated=0 on the device console.
#
# Any iteration that misses either claim fails the whole gate; a green cross-compile is no substitute for it.
#
# It never runs `idf.py monitor`: the topic-graph firmware disables the console because plexus owns UART0, and a
# monitor would hold the port the host gate needs. The onboard firmware has no link and does log, so its console is
# captured on a controlled reset instead. Run it from anywhere; paths resolve from the script's location.
#
# Usage: run_topic_propagation_gate.sh [RUNS] [PORT]
#   RUNS  number of flash+assert iterations (default 3, clamped to a minimum of 3)
#   PORT  the serial device the board enumerates as (default /dev/ttyUSB0)
#
# Environment:
#   PLEXUS_BUILD_DIR  the host build tree that holds the gate binary and receives the logs (default build)

set -euo pipefail

RUNS="${1:-3}"
PORT="${2:-/dev/ttyUSB0}"

if [[ "${RUNS}" -lt 3 ]]; then
    echo "run count ${RUNS} is below the minimum of 3 reproducible runs; clamping to 3"
    RUNS=3
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GRAPH_PROJECT="${REPO_ROOT}/examples/mcu/esp-idf-topic-graph"
ONBOARD_PROJECT="${REPO_ROOT}/examples/mcu/esp-idf-onboard"
BUILD_DIR="${PLEXUS_BUILD_DIR:-${REPO_ROOT}/build}"
HOST_GATE="${BUILD_DIR}/examples/mcu/topic_propagation_gate_host"
LOG_DIR="${BUILD_DIR}/examples/mcu/topic_propagation_gate_logs"

if [[ ! -x "${HOST_GATE}" ]]; then
    echo "host gate binary not found at ${HOST_GATE}"
    echo "build it first: cmake -S . -B build -DPLEXUS_BUILD_EXAMPLES=ON && cmake --build build -j4 --target topic_propagation_gate_host"
    exit 1
fi

# Resets the board into RUN through the auto-reset circuit (EN/IO0 on the adapter's RTS/DTR): IO0 high to run the app,
# not the download ROM, then pulse EN. The same open captures the console, so one open is one controlled reset.
reset_and_capture() {   # $1=port  $2=device-log  $3=capture-seconds
    python3 - "$1" "$2" "$3" <<'PY'
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

# The esptool auto-reset into the bootloader is flaky on this adapter ("chip stopped responding"), and the gate is about
# the wire, not the cable, so the flash is retried. Running out of retries still fails the gate.
flash_firmware() {   # $1=project-dir  $2=build-dir  $3=label
    for attempt in 1 2 3 4 5; do
        if idf.py -C "$1" -B "$2" -p "${PORT}" flash >>"${LOG_DIR}/flash.log" 2>&1; then
            echo "    flashed $3 on attempt ${attempt}"
            return 0
        fi
        echo "    flash attempt ${attempt} for $3 failed; settling then retrying"
        sleep 3
    done
    return 1
}

# The ESP-IDF install location is host-specific (/opt/esp-idf in CI, often $IDF_PATH or $HOME/esp on a workstation),
# so the known locations are tried in turn.
if ! command -v idf.py >/dev/null 2>&1; then
    for candidate in "${IDF_PATH:+${IDF_PATH}/export.sh}" "${HOME}/esp/export.sh" "${HOME}/esp/esp-idf/export.sh" /opt/esp-idf/export.sh; do
        if [[ -n "${candidate}" && -f "${candidate}" ]]; then
            # shellcheck disable=SC1090
            . "${candidate}"
            break
        fi
    done
fi

mkdir -p "${LOG_DIR}"
: >"${LOG_DIR}/flash.log"

# The cross-build proves the graph headers carry no platform fork; it is not the gate.
for project in "${GRAPH_PROJECT}" "${ONBOARD_PROJECT}"; do
    echo "=== cross-building $(basename "${project}") for esp32 ==="
    idf.py -C "${project}" -B "${project}/build_esp32" set-target esp32
    idf.py -C "${project}" -B "${project}/build_esp32" build -- -j4
done

passes=0
for ((run = 1; run <= RUNS; ++run)); do
    echo "=== run ${run}/${RUNS}: propagation — flashing the topic-graph firmware on ${PORT} ==="
    flash_firmware "${GRAPH_PROJECT}" "${GRAPH_PROJECT}/build_esp32" "topic-graph" || {
        echo "GATE: run ${run}/${RUNS} could not flash the topic-graph firmware"
        echo "GATE: ${passes}/${RUNS} runs passed — FAIL"
        exit 1
    }

    echo "=== run ${run}/${RUNS}: asserting both ends enumerate the other's topic-with-type ==="
    if ! "${HOST_GATE}" "${PORT}@115200"; then
        echo "GATE: run ${run}/${RUNS} did NOT propagate topics-with-types both ways"
        echo "GATE: ${passes}/${RUNS} runs passed — FAIL"
        exit 1
    fi

    echo "=== run ${run}/${RUNS}: direct-only — flashing the onboard firmware on ${PORT} ==="
    flash_firmware "${ONBOARD_PROJECT}" "${ONBOARD_PROJECT}/build_esp32" "onboard" || {
        echo "GATE: run ${run}/${RUNS} could not flash the onboard firmware"
        echo "GATE: ${passes}/${RUNS} runs passed — FAIL"
        exit 1
    }

    devlog="${LOG_DIR}/run_${run}_onboard.log"
    reset_and_capture "${PORT}" "${devlog}" 10
    # The ROM boot banner puts NUL bytes in the log, so -a makes grep treat it as text.
    if ! grep -aq "onboard participants snapshot: count=0 truncated=0" "${devlog}"; then
        echo "GATE: run ${run}/${RUNS} did NOT report the direct-only baseline (device serial: ${devlog})"
        echo "GATE: ${passes}/${RUNS} runs passed — FAIL"
        exit 1
    fi

    passes=$((passes + 1))
    echo "=== run ${run}/${RUNS}: both claims held ==="
    sleep 2
done

echo "GATE: ${passes}/${RUNS} runs propagated topics-with-types both ways and reported count=0 truncated=0"
exit 0
