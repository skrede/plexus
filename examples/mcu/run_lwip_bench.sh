#!/usr/bin/env bash
# The reproducible MCU bench driver: a request/echo round-trip workload at fixed payload tiers over the lwIP-P1
# (poll-drive) and lwIP-P2 (RX task) receive policies, aggregated into the standing suite table format. For each cell
# it cross-builds the bench firmware for esp32, flashes, drives the auto-reset lines into RUN, captures the device
# console, then parses the device's sample lines into p50/p99 round-trip latency plus the RX-task RAM/stack cost. It
# loops N>=3 times per cell and FAILS if any cell collects fewer than 3 valid runs; a single lucky pass never
# substantiates a number.
#
# The serial cell needs a second USB-serial adapter wired to UART1 (GPIO17 TX / GPIO16 RX) and the host serial echo
# peer serial_bench_host on the adapter's port (LINK_PORT). The host peer, the serial peer and the credentials are
# each required only for the cells actually scheduled; a missing one, or a serial pipeline request, is refused before
# the ESP-IDF export.
#
# The real SSID/password live in esp-idf-lwip-bench/main/wifi_credentials.h (gitignored, operator-supplied); this
# script never reads, hardcodes, or echoes them. Create it from the .example before running. Repoint
# PLEXUS_HOST_ENDPOINT at the host's shared-subnet IP at run time; the committed firmware default stays a
# placeholder, never the test IP.
#
# Usage: run_lwip_bench.sh [RUNS] [PORT] [HOST_PORT] [LINK_PORT]
#   RUNS       flash+capture iterations per cell (default 3, clamped to a minimum of 3)
#   PORT       the flash serial device the board enumerates as (default /dev/ttyUSB0)
#   HOST_PORT  the TCP port the host echo peer listens on (default 7447, must match the firmware)
#   LINK_PORT  the second adapter for the serial cell's UART1 link (default /dev/ttyUSB1)
#
# Environment:
#   PLEXUS_HOST_ENDPOINT  the host's shared-subnet IP:port the firmware dials (lwIP cells only)
#   BENCH_SERIAL=1        also run the serial cell (needs the UART1 adapter + serial peer)
#   BENCH_SERIAL_ONLY=1   run ONLY the serial cell (no Wi-Fi / no TCP peer needed)
#   BENCH_WORKLOAD        latency (default): request/echo round trips; oneway: a saturating device->host stream
#                         measured by the host-delivered rate, with the device shed count as the saturation witness
#                         (with BENCH_SERIAL_ONLY=1, one cell per link baud); pipeline: a window-depth K sweep over
#                         lwIP-P2 (refused with BENCH_SERIAL_ONLY=1, exit 2)
#   BENCH_DEEP_EGRESS=1   with oneway over lwIP, also run a cell built with the deep egress cap
#   LINK_PORT             the serial adapter port when arg 4 is not given (default /dev/ttyUSB1)

set -euo pipefail

RUNS="${1:-3}"
PORT="${2:-/dev/ttyUSB0}"
HOST_PORT="${3:-7447}"
LINK_PORT="${4:-${LINK_PORT:-/dev/ttyUSB1}}"

if [[ "${RUNS}" -lt 3 ]]; then
    echo "run count ${RUNS} is below the minimum of 3 reproducible runs; clamping to 3"
    RUNS=3
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IDF_PROJECT="${REPO_ROOT}/examples/mcu/esp-idf-lwip-bench"
HOST_PEER="${REPO_ROOT}/build/examples/mcu/lwip_bench_host"
SERIAL_PEER="${REPO_ROOT}/build/examples/mcu/serial_bench_host"
CREDENTIALS="${IDF_PROJECT}/main/wifi_credentials.h"
LOG_DIR="${REPO_ROOT}/build/examples/mcu/lwip_bench_logs"
SAMPLES="${LOG_DIR}/samples.txt"
THRU_SAMPLES="${LOG_DIR}/throughput.txt"
HOST_SAMPLES="${LOG_DIR}/host_throughput.txt"

. "${SCRIPT_DIR}/lwip_bench/cells.sh"
. "${SCRIPT_DIR}/lwip_bench/capture.sh"

# shellcheck disable=SC1091
. /opt/esp-idf/export.sh

mkdir -p "${LOG_DIR}"
: >"${SAMPLES}"
: >"${THRU_SAMPLES}"
: >"${HOST_SAMPLES}"

for cell in "${CELLS[@]}"; do
    echo "=== cross-building bench firmware for esp32 (cell ${cell}) ==="
    derive_build_defines "${cell}"
    # set-target fullcleans the build directory, so no cell's define leaks into the next cell.
    idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" set-target esp32
    idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" \
        -DBENCH_TRANSPORT="${build_transport}" -DBENCH_WORKLOAD="${WORKLOAD}" ${deep_def} ${window_def} ${baud_def} -DPLEXUS_HOST_ENDPOINT="${PLEXUS_HOST_ENDPOINT:-}" build

    : >"${LOG_DIR}/host_${cell}.txt"
    valid_runs=0
    for ((run = 1; run <= RUNS; ++run)); do
        log="${LOG_DIR}/${cell}_run_${run}.log"
        devlog="${LOG_DIR}/${cell}_run_${run}_device.log"

        echo "=== ${cell} run ${run}/${RUNS}: flashing ${PORT} ==="
        if ! flash_board "${cell}" "${run}" "${log}"; then continue; fi

        cap_secs=90
        # The serial cell's 4096 B tier round-trips in ~0.8 s at 115200, so its full sweep needs ~120 s.
        [[ "${cell}" == serial* ]] && cap_secs=180
        # One-way sweeps three fixed time windows (~9 s) plus dial/subscription propagation.
        [[ "${WORKLOAD}" == "oneway" ]] && cap_secs=45
        reset_and_capture "${PORT}" "${devlog}" "${cap_secs}" &
        cap_pid=$!

        if [[ "${cell}" == serial* ]]; then
            cell_baud=115200
            if [[ "${cell}" =~ ^serial-b([0-9]+)$ ]]; then cell_baud="${BASH_REMATCH[1]}"; fi
            echo "=== ${cell} run ${run}/${RUNS}: starting host serial echo peer on ${LINK_PORT} @ ${cell_baud} ==="
            "${SERIAL_PEER}" "${LINK_PORT}" "${cell_baud}" >"${log}" 2>&1 &
        else
            echo "=== ${cell} run ${run}/${RUNS}: starting host echo peer on tcp 0.0.0.0:${HOST_PORT} ==="
            "${HOST_PEER}" "${HOST_PORT}" >"${log}" 2>&1 &
        fi
        peer_pid=$!

        wait "${cap_pid}" 2>/dev/null || true
        kill "${peer_pid}" 2>/dev/null || true
        wait "${peer_pid}" 2>/dev/null || true

        if [[ "${WORKLOAD}" == "oneway" ]]; then
            validate_oneway_run "${cell}" "${run}" "${log}" "${devlog}"
        else
            validate_sweep_run "${cell}" "${run}" "${devlog}"
        fi
        sleep 3
    done

    if [[ "${valid_runs}" -lt 3 ]]; then
        echo "BENCH: cell ${cell} produced ${valid_runs}/${RUNS} valid runs — below the minimum of 3, FAIL"
        exit 1
    fi
    echo "BENCH: cell ${cell} produced ${valid_runs}/${RUNS} valid runs"
done

if [[ "${WORKLOAD}" == "oneway" ]]; then
    python3 "${SCRIPT_DIR}/lwip_bench/aggregate_oneway.py" "${THRU_SAMPLES}" "${LOG_DIR}" "${CELLS[@]}"
    echo "BENCH: all cells produced >=3 valid runs"
    exit 0
fi

if [[ "${WORKLOAD}" == "pipeline" ]]; then
    python3 "${SCRIPT_DIR}/lwip_bench/aggregate_pipeline.py" "${THRU_SAMPLES}" "${SAMPLES}" "${LOG_DIR}"
    echo "BENCH: all cells produced >=3 valid runs"
    exit 0
fi

python3 "${SCRIPT_DIR}/lwip_bench/aggregate_latency.py" "${SAMPLES}" "${LOG_DIR}"

echo "BENCH: all cells produced >=3 valid runs"
exit 0
