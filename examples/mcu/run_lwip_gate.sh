#!/usr/bin/env bash
# The live lwIP gate for the on-device lwIP examples. It cross-builds the firmware for esp32, then loops N>=3 times.
# The device is the dialer: it boots, joins the network, takes a DHCP lease and dials the fixed host endpoint. Each
# iteration flashes the board, drives the auto-reset lines into RUN, captures the device serial log to a per-run file
# and runs the host TCP server CYCLES times in turn, each instance asserting the device's first message or timing out.
# Any host instance that misses the message fails the whole gate; one lucky pass is no link claim.
#
# For a Wi-Fi project the SSID and password live in main/wifi_credentials.h (gitignored, operator-supplied); this
# script only checks that the file exists and never reads, hardcodes or echoes it. Create it from the .example before
# running. Run it from anywhere; paths resolve from the script's location.
#
# Usage: run_lwip_gate.sh [RUNS] [PORT] [HOST_PORT]
#   RUNS       number of flash+assert iterations (default 3, clamped to a minimum of 3)
#   PORT       the flash serial device the board enumerates as (default /dev/ttyUSB0)
#   HOST_PORT  the TCP port the host server listens on (default 7447, must match the firmware)
#
# Environment:
#   IDF_PROJECT      the ESP-IDF project to build and flash (default examples/mcu/esp-idf-lwip)
#   BUILD_DIR        its build directory (default ${IDF_PROJECT}/build_esp32); it holds a stamp of the
#                    IDF_PROJECT, IDF_DEFINES and SDKCONFIG_FILE of its last successful build, and a run whose
#                    knobs differ from the stamp empties it with idf.py fullclean first, so a define left out is
#                    really left out; the script refuses a BUILD_DIR that holds SDKCONFIG_FILE, LOG_DIR or the
#                    host gate binary, one whose CMake cache belongs to another source tree, and, without
#                    SDKCONFIG_FILE, one holding files but no CMake cache
#   SDKCONFIG_FILE   when set, build with this sdkconfig and IDF_TARGET=esp32 instead of running
#                    set-target, which would regenerate the project's sdkconfig; a relative path is
#                    resolved against IDF_PROJECT, the way the ESP-IDF build resolves it
#   IDF_DEFINES      extra -D words passed to the build and to flash, e.g.
#                    "-DPLEXUS_HOST_ENDPOINT=192.168.1.10:7447 -DPLEXUS_PUBLISH_PERIOD_MS=10"; they are printed
#                    before the cross-build and written to LOG_DIR/run_<n>_defines.log before each flash
#   CAPTURE_SECONDS  the device serial capture window per run (default 75)
#   CYCLES           host instances run one after another within a run, each must pass; the device
#                    redials after each one exits (default 1)
#   LOG_DIR          where the per-run logs go (default build/examples/mcu/lwip_gate_logs)
#   PLEXUS_HOST_HOLD and PLEXUS_HOST_DROP_AFTER pass through to every host instance.

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
IDF_PROJECT="$(cd "${IDF_PROJECT:-${REPO_ROOT}/examples/mcu/esp-idf-lwip}" && pwd -P)"
BUILD_DIR="${BUILD_DIR:-${IDF_PROJECT}/build_esp32}"
SDKCONFIG_FILE="${SDKCONFIG_FILE:-}"
if [[ -n "${SDKCONFIG_FILE}" && "${SDKCONFIG_FILE}" != /* ]]; then SDKCONFIG_FILE="${IDF_PROJECT}/${SDKCONFIG_FILE}"; fi
CAPTURE_SECONDS="${CAPTURE_SECONDS:-75}"
CYCLES="${CYCLES:-1}"
LOG_DIR="${LOG_DIR:-${REPO_ROOT}/build/examples/mcu/lwip_gate_logs}"
HOST_GATE="${REPO_ROOT}/build/examples/mcu/lwip_gate_host"
CREDENTIALS="${IDF_PROJECT}/main/wifi_credentials.h"
CREDENTIALS_TEMPLATE="${IDF_PROJECT}/main/wifi_credentials.h.example"
KNOB_STAMP="${BUILD_DIR}/plexus_gate_knobs.txt"
KNOBS="IDF_PROJECT=${IDF_PROJECT} IDF_DEFINES=${IDF_DEFINES:-} SDKCONFIG_FILE=${SDKCONFIG_FILE}"

read -r -a EXTRA_DEFINES <<< "${IDF_DEFINES:-}"
IDF_ARGS=(-C "${IDF_PROJECT}" -B "${BUILD_DIR}")
if [[ -n "${SDKCONFIG_FILE}" ]]; then
    IDF_ARGS+=("-DSDKCONFIG=${SDKCONFIG_FILE}" "-DIDF_TARGET=esp32")
fi
IDF_ARGS+=("${EXTRA_DEFINES[@]}")

if [[ "${CYCLES}" -lt 1 ]]; then
    echo "CYCLES=${CYCLES} must be at least 1"
    exit 1
fi

if [[ ! -x "${HOST_GATE}" ]]; then
    echo "host gate binary not found at ${HOST_GATE}"
    echo "build it first: cmake --build build -j4 --target lwip_gate_host"
    exit 1
fi

if [[ -f "${CREDENTIALS_TEMPLATE}" && ! -f "${CREDENTIALS}" ]]; then
    echo "Wi-Fi credentials not found at ${CREDENTIALS}"
    echo "create it from the template (it is gitignored and never committed):"
    echo "  cp ${CREDENTIALS_TEMPLATE} ${CREDENTIALS}   # then fill in SSID/password"
    exit 1
fi

# idf.py fullclean empties BUILD_DIR and set-target runs it on every default-path run, so nothing the gate keeps may
# live inside it and only a CMake build of IDF_PROJECT is cleaned. python3 resolves paths that do not exist yet, and
# it answers outside with 3 because a missing or failing interpreter exits 127 or 1.
inside_build_dir() { python3 -c 'import os, sys; b, p = map(os.path.realpath, sys.argv[1:3]); sys.exit(0 if os.path.commonpath([b, p]) == b else 3)' "${BUILD_DIR}" "$1"; }
for path in ${SDKCONFIG_FILE:+"${SDKCONFIG_FILE}"} "${LOG_DIR}" "${HOST_GATE}"; do
    rc=0; inside_build_dir "${path}" || rc=$?
    if [[ "${rc}" -eq 0 ]]; then echo "${path} lies inside BUILD_DIR=${BUILD_DIR}, which idf.py fullclean empties; choose a BUILD_DIR that does not contain it"; exit 1; fi
    if [[ "${rc}" -ne 3 ]]; then echo "cannot tell whether ${path} lies inside BUILD_DIR=${BUILD_DIR}: python3 exited ${rc}"; exit 1; fi
done
if [[ -f "${BUILD_DIR}/CMakeCache.txt" ]] && ! grep -qxF "CMAKE_HOME_DIRECTORY:INTERNAL=${IDF_PROJECT}" "${BUILD_DIR}/CMakeCache.txt"; then
    echo "BUILD_DIR=${BUILD_DIR} holds a CMake build of another source tree than ${IDF_PROJECT}; delete it or choose another BUILD_DIR"
    exit 1
fi

# Resets the board into RUN through the auto-reset circuit (EN/IO0 on the adapter's RTS/DTR): IO0 high to run the app,
# not the download ROM, then pulse EN. The same open captures the console at 115200, so a missed receipt is
# diagnosable (link, DHCP lease, dial), and no second port open resets the board mid-run.
reset_and_capture() {   # $1=port  $2=device-log  $3=capture-seconds
    # exec makes the background job python itself; killing a wrapping subshell would orphan python, holding the port.
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

stop_capture() {   # $1=capture pid
    kill "$1" 2>/dev/null || true
    wait "$1" 2>/dev/null || true
}

# shellcheck disable=SC1091
. /opt/esp-idf/export.sh

echo "=== cross-building ${IDF_PROJECT} for esp32 ==="
echo "effective defines: ${IDF_DEFINES:-<none>}"
# A -D cache entry outlives the run that set it, so a changed knob set starts from a clean build directory.
# The stamp is dropped before the build and written after it succeeds, so a failed build leaves none.
if [[ -f "${BUILD_DIR}/CMakeCache.txt" && "$(cat "${KNOB_STAMP}" 2>/dev/null)" != "${KNOBS}" ]]; then
    echo "the build knobs differ from, or were never stamped by, the last successful build; cleaning ${BUILD_DIR}"
    idf.py "${IDF_ARGS[@]}" fullclean
fi
rm -f "${KNOB_STAMP}"
if [[ -z "${SDKCONFIG_FILE}" ]]; then
    if [[ -d "${BUILD_DIR}" && ! -f "${BUILD_DIR}/CMakeCache.txt" && -n "$(ls -A "${BUILD_DIR}")" ]]; then
        echo "BUILD_DIR=${BUILD_DIR} holds files but no CMake cache, which idf.py set-target refuses to clean; delete it and rerun"
        exit 1
    fi
    idf.py "${IDF_ARGS[@]}" set-target esp32
fi
idf.py "${IDF_ARGS[@]}" build
printf '%s\n' "${KNOBS}" > "${KNOB_STAMP}"

mkdir -p "${LOG_DIR}"

passes=0
for ((run = 1; run <= RUNS; ++run)); do
    # Flash and reset first, then start the host server: the device's boot, join and DHCP take seconds while the
    # server listens within about 1 s, so it still wins the race, and the receive deadline excludes the ~20 s flash.
    echo "=== run ${run}/${RUNS}: flashing ${PORT} ==="
    printf 'IDF_DEFINES=%s\nSDKCONFIG_FILE=%s\n' "${IDF_DEFINES:-}" "${SDKCONFIG_FILE}" >"${LOG_DIR}/run_${run}_defines.log"
    idf.py "${IDF_ARGS[@]}" -p "${PORT}" flash >"${LOG_DIR}/run_${run}_flash.log" 2>&1

    devlog="${LOG_DIR}/run_${run}_device.log"
    reset_and_capture "${PORT}" "${devlog}" "${CAPTURE_SECONDS}" &
    cap_pid=$!

    for ((cycle = 1; cycle <= CYCLES; ++cycle)); do
        log="${LOG_DIR}/run_${run}_host_${cycle}.log"
        echo "=== run ${run}/${RUNS} cycle ${cycle}/${CYCLES}: host server on tcp 0.0.0.0:${HOST_PORT} ==="
        if ! "${HOST_GATE}" "${HOST_PORT}" >"${log}" 2>&1; then
            stop_capture "${cap_pid}"
            echo "GATE: run ${run}/${RUNS} cycle ${cycle}/${CYCLES} did NOT receive the message (host: ${log}; device serial: ${devlog})"
            echo "GATE: ${passes}/${RUNS} runs received the message — FAIL"
            exit 1
        fi
    done

    passes=$((passes + 1))
    stop_capture "${cap_pid}"
    # The capture held the port; without this pause esptool races the tty release ("multiple access on port").
    sleep 3
done

echo "GATE: ${passes}/${RUNS} runs received the message"
exit 0
