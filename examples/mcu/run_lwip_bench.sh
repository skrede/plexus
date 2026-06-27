#!/usr/bin/env bash
# The reproducible MCU bench driver: a request/echo round-trip workload at fixed payload tiers over
# the lwIP-P1 (poll-drive) and lwIP-P2 (RX task) receive policies, aggregated into the standing suite
# table format. For each cell it cross-builds the bench firmware for esp32 (the on-target compile
# proof), flashes, drives the auto-reset lines into RUN, captures the device console, then parses the
# device's machine-parseable sample lines into p50/p99 round-trip latency plus the RX-task RAM/stack
# cost. It loops N>=3 times per cell and FAILS if any cell collects fewer than 3 valid runs — a
# single lucky pass never substantiates a number. It does NOT pre-pick a winner; both policies are
# run and reported side by side.
#
# The serial cell is opt-in (BENCH_SERIAL=1): it needs a second USB-serial adapter wired to UART1
# (GPIO17 TX / GPIO16 RX) and the serial echo peer, beyond the Wi-Fi setup the lwIP cells use.
#
# The real SSID/password live in esp-idf-lwip-bench/main/wifi_credentials.h (gitignored,
# operator-supplied); this script never reads, hardcodes, or echoes them. Create it from the
# .example before running. Repoint PLEXUS_HOST_ENDPOINT at the host's shared-subnet IP at run time;
# the committed firmware default stays a placeholder, never the test IP.
#
# Usage: run_lwip_bench.sh [RUNS] [PORT] [HOST_PORT]
#   RUNS       flash+capture iterations per cell (default 3, clamped to a minimum of 3)
#   PORT       the flash serial device the board enumerates as (default /dev/ttyUSB0)
#   HOST_PORT  the TCP port the host echo peer listens on (default 7447, must match the firmware)
#
# Environment:
#   PLEXUS_HOST_ENDPOINT  the host's shared-subnet IP:port the firmware dials (required for a run)
#   BENCH_SERIAL=1        also run the serial cell (needs the UART1 adapter + serial peer)

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
IDF_PROJECT="${REPO_ROOT}/examples/mcu/esp-idf-lwip-bench"
HOST_PEER="${REPO_ROOT}/build/examples/mcu/lwip_bench_host"
CREDENTIALS="${IDF_PROJECT}/main/wifi_credentials.h"
LOG_DIR="${REPO_ROOT}/build/examples/mcu/lwip_bench_logs"
SAMPLES="${LOG_DIR}/samples.txt"

CELLS=(lwip-p1 lwip-p2)
if [[ "${BENCH_SERIAL:-0}" == "1" ]]; then
    CELLS=(serial lwip-p1 lwip-p2)
fi

if [[ ! -x "${HOST_PEER}" ]]; then
    echo "host bench peer not found at ${HOST_PEER}"
    echo "build it first: cmake --build build -j4 --target lwip_bench_host"
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
# device console (115200) to a per-run log for the capture window, so the sample lines and any join /
# DHCP / dial diagnostics land in one file. One open means one controlled reset, no second port-open
# re-resetting the board mid-run.
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

mkdir -p "${LOG_DIR}"
: >"${SAMPLES}"

for cell in "${CELLS[@]}"; do
    echo "=== cross-building bench firmware for esp32 (cell ${cell}) ==="
    idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" set-target esp32
    idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" \
        -DBENCH_TRANSPORT="${cell}" -DPLEXUS_HOST_ENDPOINT="${PLEXUS_HOST_ENDPOINT:-}" build

    valid_runs=0
    for ((run = 1; run <= RUNS; ++run)); do
        log="${LOG_DIR}/${cell}_run_${run}.log"
        devlog="${LOG_DIR}/${cell}_run_${run}_device.log"

        echo "=== ${cell} run ${run}/${RUNS}: flashing ${PORT} ==="
        # The dev-board auto-reset-into-bootloader (esptool DTR/RTS) is intermittently flaky on this
        # adapter ("chip stopped responding" ~1 in 3); retry the flash so one stuck reset does not abort
        # the whole sweep. Mirrors the startup-race robustness the round-trip workload already carries.
        flashed=0
        for attempt in 1 2 3 4 5; do
            if idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" -p "${PORT}" flash >>"${log}" 2>&1; then
                flashed=1; break
            fi
            echo "=== ${cell} run ${run}/${RUNS}: flash attempt ${attempt} failed; settling then retrying ==="
            sleep 3
        done
        if [[ "${flashed}" -ne 1 ]]; then
            echo "=== ${cell} run ${run}/${RUNS}: flash failed after retries (board not entering download mode) ==="
            sleep 3
            continue
        fi

        reset_and_capture "${PORT}" "${devlog}" 90 &
        cap_pid=$!

        echo "=== ${cell} run ${run}/${RUNS}: starting host echo peer on tcp 0.0.0.0:${HOST_PORT} ==="
        "${HOST_PEER}" "${HOST_PORT}" >"${log}" 2>&1 &
        peer_pid=$!

        # Hold the capture window, then tear the peer down. The device emits a BENCH resource line
        # once it has swept every tier; its presence in the capture marks a complete run.
        wait "${cap_pid}" 2>/dev/null || true
        kill "${peer_pid}" 2>/dev/null || true
        wait "${peer_pid}" 2>/dev/null || true

        if grep -q "BENCH resource policy=${cell}" "${devlog}"; then
            grep -E "^BENCH sample policy=${cell} " "${devlog}" >>"${SAMPLES}" || true
            valid_runs=$((valid_runs + 1))
            echo "=== ${cell} run ${run}/${RUNS}: complete sweep captured ==="
        else
            echo "=== ${cell} run ${run}/${RUNS}: NO complete sweep (device serial: ${devlog}) ==="
        fi
        sleep 3
    done

    if [[ "${valid_runs}" -lt 3 ]]; then
        echo "BENCH: cell ${cell} produced ${valid_runs}/${RUNS} valid runs — below the minimum of 3, FAIL"
        exit 1
    fi
    echo "BENCH: cell ${cell} produced ${valid_runs}/${RUNS} valid runs"
done

# Aggregate p50/p99 per (cell x tier) and emit the standing suite table format: a plexus-only
# transport x payload table, then one table per transport type. p50/p99 are nearest-rank order
# statistics over every sampled round trip across all valid runs of the cell.
python3 - "${SAMPLES}" "${LOG_DIR}" <<'PY'
import sys, glob, os, re, statistics

samples_path, log_dir = sys.argv[1], sys.argv[2]

rtt = {}    # (cell, tier) -> [rtt_us...]
with open(samples_path) as f:
    for line in f:
        m = re.match(r"BENCH sample policy=(\S+) tier=(\d+) index=\d+ rtt_us=(-?\d+)", line)
        if m:
            rtt.setdefault((m.group(1), int(m.group(2))), []).append(int(m.group(3)))

resource = {}   # cell -> (rx_stack_words, free_heap)
for dev in glob.glob(os.path.join(log_dir, "*_device.log")):
    with open(dev, errors="ignore") as f:
        for line in f:
            m = re.match(r"BENCH resource policy=(\S+) rx_stack_words=(\d+) free_heap=(\d+)", line)
            if m:
                resource[m.group(1)] = (int(m.group(2)), int(m.group(3)))

cells = [c for c in ("serial", "lwip-p1", "lwip-p2") if any(k[0] == c for k in rtt)]
tiers = sorted({k[1] for k in rtt})

def pct(values, q):
    s = sorted(values)
    if not s:
        return None
    rank = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
    return s[rank]

def fmt(v):
    return "-" if v is None else f"{v}"

print()
print("## Round-trip latency p50 (us) — transport x payload")
print()
header = "| transport | " + " | ".join(f"{t} B" for t in tiers) + " |"
print(header)
print("| " + " | ".join(["---"] * (len(tiers) + 1)) + " |")
best = {t: min((pct(rtt[(c, t)], 0.50) for c in cells if (c, t) in rtt), default=None) for t in tiers}
for c in cells:
    row = [c]
    for t in tiers:
        v = pct(rtt.get((c, t), []), 0.50)
        cell = "-" if v is None else (f"**{v}**" if v == best[t] else f"{v}")
        row.append(cell)
    print("| " + " | ".join(row) + " |")

for c in cells:
    print()
    print(f"## {c} — p50 / p99 round-trip latency (us) + RX-task RAM/stack")
    print()
    print("| payload | p50 us | p99 us | samples |")
    print("| --- | --- | --- | --- |")
    for t in tiers:
        vals = rtt.get((c, t), [])
        print(f"| {t} B | {fmt(pct(vals, 0.50))} | {fmt(pct(vals, 0.99))} | {len(vals)} |")
    rx_stack, free_heap = resource.get(c, (0, 0))
    print()
    print(f"RX-task stack high-water: {rx_stack} words | free heap at sweep end: {free_heap} B")

if "lwip-p1" in resource and "lwip-p2" in resource:
    delta = resource["lwip-p1"][1] - resource["lwip-p2"][1]
    print()
    print(f"RX-task incremental heap cost (P1 free heap - P2 free heap): {delta} B")
PY

echo "BENCH: all cells produced >=3 valid runs"
exit 0
