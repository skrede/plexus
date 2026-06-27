#!/usr/bin/env bash
# The reproducible onboard intra-node zero-copy bench driver: a request/echo round-trip workload at
# fixed payload tiers over the node's OWN intra-node self-route — zero link, no Wi-Fi, no host peer,
# no second node. It cross-builds the bench firmware for esp32 (the on-target compile proof),
# flashes, drives the auto-reset lines into RUN, captures the device console, then parses the
# device's machine-parseable sample lines into p50/p99 round-trip latency plus the zero-copy witness
# (the codec's encode count, which must be 0) and the free-heap reading. It loops N>=3 times and
# FAILS if fewer than 3 valid runs are collected — a single lucky pass never substantiates a number.
#
# Usage: run_onboard_bench.sh [RUNS] [PORT]
#   RUNS  flash+capture iterations (default 3, clamped to a minimum of 3)
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
IDF_PROJECT="${REPO_ROOT}/examples/mcu/esp-idf-onboard-bench"
BUILD_DIR="${IDF_PROJECT}/build_esp32"
LOG_DIR="${REPO_ROOT}/build/examples/mcu/onboard_bench_logs"
SAMPLES="${LOG_DIR}/samples.txt"

CELL="intra-onboard"

# Reset the board into RUN via the dev-board auto-reset circuit (EN/IO0 tied to the adapter's
# RTS/DTR): IO0 high (run, not the download ROM), then pulse EN low->high. The SAME open captures the
# device console (115200) for the capture window, so every BENCH line lands in one file. One open
# means one controlled reset — no second port-open re-resetting the board mid-run.
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

echo "=== cross-building onboard bench firmware for esp32 ==="
idf.py -C "${IDF_PROJECT}" -B "${BUILD_DIR}" set-target esp32
idf.py -C "${IDF_PROJECT}" -B "${BUILD_DIR}" build -- -j4

valid_runs=0
for ((run = 1; run <= RUNS; ++run)); do
    log="${LOG_DIR}/run_${run}.log"
    devlog="${LOG_DIR}/run_${run}_device.log"

    echo "=== run ${run}/${RUNS}: flashing ${PORT} ==="
    # The dev-board auto-reset-into-bootloader (esptool DTR/RTS) is intermittently flaky on this
    # adapter ("chip stopped responding" ~1 in 3); retry the flash so one stuck reset does not abort
    # the whole sweep.
    flashed=0
    for attempt in 1 2 3 4 5; do
        if idf.py -C "${IDF_PROJECT}" -B "${BUILD_DIR}" -p "${PORT}" flash >>"${log}" 2>&1; then
            flashed=1
            echo "=== run ${run}/${RUNS}: flashed on attempt ${attempt} ==="
            break
        fi
        echo "=== run ${run}/${RUNS}: flash attempt ${attempt} failed; settling then retrying ==="
        sleep 3
    done
    if [[ "${flashed}" -ne 1 ]]; then
        echo "=== run ${run}/${RUNS}: flash failed after retries (board not entering download mode) ==="
        sleep 3
        continue
    fi

    # No network join, no peer dial: the sweep starts the moment the app runs, so a 30 s window is
    # ample for 3 tiers x (8 warmup + 128 sampled) self-route round trips. Backgrounded so the
    # function's exec replaces only the subshell, not this driver; wait for the window to close.
    reset_and_capture "${PORT}" "${devlog}" 30 &
    wait "$!" 2>/dev/null || true

    # The console log carries NUL bytes from the ROM boot banner; -a forces grep to treat it as text
    # so the BENCH lines match (without -a grep reports a binary match and emits no lines).
    if grep -aq "BENCH resource policy=${CELL}" "${devlog}"; then
        grep -aE "^BENCH sample policy=${CELL} " "${devlog}" >>"${SAMPLES}" || true
        valid_runs=$((valid_runs + 1))
        echo "=== run ${run}/${RUNS}: complete sweep captured ==="
    else
        echo "=== run ${run}/${RUNS}: NO complete sweep (device serial: ${devlog}) ==="
    fi
    sleep 3
done

if [[ "${valid_runs}" -lt 3 ]]; then
    echo "BENCH: produced ${valid_runs}/${RUNS} valid runs — below the minimum of 3, FAIL"
    exit 1
fi
echo "BENCH: produced ${valid_runs}/${RUNS} valid runs"

# Aggregate p50/p99 per tier and emit the standing suite table: a one-cell intra-node transport x
# payload table, then the per-tier p50/p99 detail plus the zero-copy witness and free heap. p50/p99
# are nearest-rank order statistics over every sampled round trip across all valid runs.
python3 - "${SAMPLES}" "${LOG_DIR}" "${CELL}" <<'PY'
import sys, glob, os, re

samples_path, log_dir, cell = sys.argv[1], sys.argv[2], sys.argv[3]

rtt = {}    # tier -> [rtt_us...]
with open(samples_path) as f:
    for line in f:
        m = re.match(r"BENCH sample policy=(\S+) tier=(\d+) index=\d+ rtt_us=(-?\d+)", line)
        if m:
            rtt.setdefault(int(m.group(2)), []).append(int(m.group(3)))

witness = None
free_heap = None
for dev in glob.glob(os.path.join(log_dir, "*_device.log")):
    with open(dev, errors="ignore") as f:
        for line in f:
            mw = re.match(r"BENCH witness policy=(\S+) encodes=(-?\d+)", line)
            if mw:
                witness = max(witness or 0, int(mw.group(2)))
            mr = re.match(r"BENCH resource policy=(\S+) free_heap=(\d+)", line)
            if mr:
                free_heap = int(mr.group(2))

tiers = sorted(rtt)

def pct(values, q):
    s = sorted(values)
    if not s:
        return None
    rank = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
    return s[rank]

def fmt(v):
    return "-" if v is None else f"{v}"

print()
print("## Onboard intra-node round-trip latency p50 (us) — transport x payload")
print()
print("| transport | " + " | ".join(f"{t} B" for t in tiers) + " |")
print("| " + " | ".join(["---"] * (len(tiers) + 1)) + " |")
print("| " + cell + " | " + " | ".join(fmt(pct(rtt[t], 0.50)) for t in tiers) + " |")

print()
print(f"## {cell} — p50 / p99 round-trip latency (us)")
print()
print("| payload | p50 us | p99 us | samples |")
print("| --- | --- | --- | --- |")
for t in tiers:
    vals = rtt[t]
    print(f"| {t} B | {fmt(pct(vals, 0.50))} | {fmt(pct(vals, 0.99))} | {len(vals)} |")

print()
print(f"zero-copy witness (codec encode count, MUST be 0): {witness}")
print(f"free heap at sweep end: {free_heap} B")
if witness not in (0, None):
    print("WITNESS FAILURE: encode was invoked on the intra-node typed path — NOT zero-copy")
PY

echo "BENCH: produced >=3 valid runs"
exit 0
