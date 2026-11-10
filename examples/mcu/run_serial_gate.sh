#!/usr/bin/env bash
# The live serial gate for the on-device serial example. It cross-builds the firmware for esp32, then loops N>=3
# times: flash the board, then run the host gate, which dials the device over the serial link, subscribes and asserts
# receipt of one message. Any iteration that misses the message fails the whole gate; one lucky pass is no link claim.
#
# It never runs `idf.py monitor`: plexus owns UART0, so the console is disabled, and a monitor would hold the port the
# host gate needs. Run it from anywhere; paths resolve from the script's location.
#
# Usage: run_serial_gate.sh [RUNS] [PORT]
#   RUNS  number of flash+assert iterations (default 3, clamped to a minimum of 3)
#   PORT  the serial device the board enumerates as (default /dev/ttyUSB0)

set -euo pipefail

RUNS="${1:-3}"
PORT="${2:-/dev/ttyUSB0}"

if [[ "${RUNS}" -lt 3 ]]; then
    echo "run count ${RUNS} is below the minimum of 3 reproducible runs; clamping to 3"
    RUNS=3
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IDF_PROJECT="${REPO_ROOT}/examples/mcu/esp-idf"
HOST_GATE="${REPO_ROOT}/build/examples/mcu/serial_gate_host"

if [[ ! -x "${HOST_GATE}" ]]; then
    echo "host gate binary not found at ${HOST_GATE}"
    echo "build it first: cmake --build build -j4 --target serial_gate_host"
    exit 1
fi

# shellcheck disable=SC1091
. /opt/esp-idf/export.sh

# The cross-build is the on-target compile proof of the UART leaf and the example; it fails the gate before any flash.
echo "=== cross-building firmware for esp32 ==="
idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" set-target esp32
idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" build

# Each flash leaves the board running the app, because the CP2102 auto-resets it.
passes=0
for ((run = 1; run <= RUNS; ++run)); do
    echo "=== run ${run}/${RUNS}: flashing ${PORT} ==="
    idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" -p "${PORT}" flash

    echo "=== run ${run}/${RUNS}: asserting receipt over ${PORT} ==="
    if "${HOST_GATE}"; then
        passes=$((passes + 1))
    else
        echo "GATE: run ${run}/${RUNS} did NOT receive the message"
        echo "GATE: ${passes}/${RUNS} runs received the message — FAIL"
        exit 1
    fi
done

echo "GATE: ${passes}/${RUNS} runs received the message"
exit 0
