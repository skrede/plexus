#!/usr/bin/env bash
# The reproducible live serial gate driver for the on-device vertical slice.
#
# It cross-builds the firmware for the esp32 target (the on-target compile proof of the UART
# byte_channel + the example slice), then loops N>=3 times: each iteration flashes the board and
# runs the host gate program, which dials the device over the real serial link, completes the
# handshake, subscribes, and asserts receipt of one real message. The whole gate FAILS if ANY
# iteration misses the message — a single lucky pass is never sufficient for a link claim. On
# all-pass it prints the run tally and exits 0.
#
# It deliberately never runs `idf.py monitor`: the console is disabled (plexus owns UART0) and a
# monitor would hold the port the host gate needs. Run it from anywhere — paths resolve from the
# script's own location.
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

# Resolve the repo root from this script's location (examples/mcu/run_serial_gate.sh -> root).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IDF_PROJECT="${REPO_ROOT}/examples/mcu/esp-idf"
HOST_GATE="${REPO_ROOT}/build/examples/serial_gate_host"

if [[ ! -x "${HOST_GATE}" ]]; then
    echo "host gate binary not found at ${HOST_GATE}"
    echo "build it first: cmake --build build -j4 --target serial_gate_host"
    exit 1
fi

# Bring the cross-toolchain into scope (xtensa gcc + idf.py).
# shellcheck disable=SC1091
. /opt/esp-idf/export.sh

# The on-target compile: set the target once, then build. A non-zero exit fails the gate before
# a single flash — the cross-build IS the on-device compile proof of the UART leaf + the slice.
echo "=== cross-building firmware for esp32 ==="
idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" set-target esp32
idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" build

# The reproducible flash+assert loop. Each iteration reflashes (the CP2102 auto-resets the board
# into the app), then the host gate dials + asserts one message. Any miss fails the whole gate.
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
