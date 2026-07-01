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
# The serial cell is opt-in: BENCH_SERIAL=1 adds it alongside the lwIP cells; BENCH_SERIAL_ONLY=1
# runs it alone (no Wi-Fi, no TCP peer). It needs a second USB-serial adapter wired to UART1 (GPIO17
# TX / GPIO16 RX) and the host serial echo peer serial_bench_host on the adapter's port (LINK_PORT).
# Each guard is required only for the cells actually scheduled.
#
# The real SSID/password live in esp-idf-lwip-bench/main/wifi_credentials.h (gitignored,
# operator-supplied); this script never reads, hardcodes, or echoes them. Create it from the
# .example before running. Repoint PLEXUS_HOST_ENDPOINT at the host's shared-subnet IP at run time;
# the committed firmware default stays a placeholder, never the test IP.
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
#   LINK_PORT             override the serial adapter port (else arg 4, else /dev/ttyUSB1)

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

# latency = request/echo round trips (the default); oneway = a saturating device->host stream measured
# by the host-delivered rate with the device shed count as the saturation witness.
WORKLOAD="${BENCH_WORKLOAD:-latency}"

CELLS=(lwip-p1 lwip-p2)
if [[ "${BENCH_SERIAL_ONLY:-0}" == "1" ]]; then
    CELLS=(serial)
elif [[ "${BENCH_SERIAL:-0}" == "1" ]]; then
    CELLS=(serial lwip-p1 lwip-p2)
fi

# The one-way stream is device->host only (no ingress), so it uses the poll-drive P1 policy — no RX task
# to idle or crash. BENCH_DEEP_EGRESS=1 adds a second cell built with the deep egress cap so the table
# shows the default (8 KiB, the honest headline) and the deep cap side by side (the widen cost).
if [[ "${WORKLOAD}" == "oneway" && "${BENCH_SERIAL_ONLY:-0}" != "1" ]]; then
    CELLS=(lwip-p1)
    [[ "${BENCH_DEEP_EGRESS:-0}" == "1" ]] && CELLS=(lwip-p1 lwip-p1-deep)
fi

# The pipeline workload sweeps the window depth K over lwIP-P2 (request/reply has ingress, so it needs the
# RX task): one cell per K, each a full rebuild so BENCH_WINDOW_K is compiled in. K=1 reduces to latency.
if [[ "${WORKLOAD}" == "pipeline" && "${BENCH_SERIAL_ONLY:-0}" != "1" ]]; then
    CELLS=()
    for k in 1 2 4 8 16; do CELLS+=("lwip-p2-k${k}"); done
fi

# The serial one-way workload sweeps the link baud: one cell per rate (a rebuild with BENCH_BAUD, the host
# peer launched at the matched baud). The baud rides the cell name so the per-transport table is a
# baud x payload map, and the high FTDI rates settle whether the no-flow-control link holds without RTS/CTS.
if [[ "${WORKLOAD}" == "oneway" && "${BENCH_SERIAL_ONLY:-0}" == "1" ]]; then
    CELLS=()
    for b in 115200 460800 921600; do CELLS+=("serial-b${b}"); done
fi

has_cell() { local c; for c in "${CELLS[@]}"; do [[ "${c}" == "$1" ]] && return 0; done; return 1; }
need_lwip=0
for c in "${CELLS[@]}"; do [[ "${c}" == lwip-* ]] && need_lwip=1; done
need_serial=0
if has_cell serial; then need_serial=1; fi

if [[ "${need_lwip}" == "1" && ! -x "${HOST_PEER}" ]]; then
    echo "host bench peer not found at ${HOST_PEER}"
    echo "build it first: cmake --build build -j4 --target lwip_bench_host"
    exit 1
fi

if [[ "${need_serial}" == "1" && ! -x "${SERIAL_PEER}" ]]; then
    echo "serial bench peer not found at ${SERIAL_PEER}"
    echo "build it first: cmake --build build -j4 --target serial_bench_host"
    exit 1
fi

if [[ "${need_lwip}" == "1" && ! -f "${CREDENTIALS}" ]]; then
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
: >"${THRU_SAMPLES}"
: >"${HOST_SAMPLES}"

for cell in "${CELLS[@]}"; do
    echo "=== cross-building bench firmware for esp32 (cell ${cell}) ==="
    # The deep-egress cell is the lwip-p1 one-way build plus the deep-cap define; a pipeline cell
    # (lwip-p2-k<N>) is the lwip-p2 build plus its window define. set-target fullcleans each iteration so
    # neither define leaks into the next cell.
    build_transport="${cell}"
    deep_def=""
    window_def=""
    baud_def=""
    if [[ "${cell}" == "lwip-p1-deep" ]]; then
        build_transport="lwip-p1"
        deep_def="-DBENCH_DEEP_EGRESS=1"
    elif [[ "${cell}" =~ ^lwip-p2-k([0-9]+)$ ]]; then
        build_transport="lwip-p2"
        window_def="-DBENCH_WINDOW_K=${BASH_REMATCH[1]}"
    elif [[ "${cell}" =~ ^serial-b([0-9]+)$ ]]; then
        build_transport="serial"
        baud_def="-DBENCH_BAUD=${BASH_REMATCH[1]}"
    fi
    idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" set-target esp32
    idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" \
        -DBENCH_TRANSPORT="${build_transport}" -DBENCH_WORKLOAD="${WORKLOAD}" ${deep_def} ${window_def} ${baud_def} -DPLEXUS_HOST_ENDPOINT="${PLEXUS_HOST_ENDPOINT:-}" build

    : >"${LOG_DIR}/host_${cell}.txt"
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

        # Hold the capture window, then tear the peer down. The device emits a BENCH resource line
        # once it has swept every tier; its presence in the capture marks a complete run.
        wait "${cap_pid}" 2>/dev/null || true
        kill "${peer_pid}" 2>/dev/null || true
        wait "${peer_pid}" 2>/dev/null || true

        if [[ "${WORKLOAD}" == "oneway" ]]; then
            # A valid one-way run needs device throughput lines AND host-delivered lines. lwIP additionally
            # requires a nonzero shed (dropped>0) as the saturation witness; serial has no congestion shed
            # (its dropped is CRC-RX), so the host-delivered rate is the headline, cross-checked by device
            # offered ≈ host delivered.
            host_hit=0; if grep -q "^HOST throughput " "${log}"; then host_hit=1; fi
            dev_hit=0
            if [[ "${cell}" == serial* ]]; then
                if grep -aqE "^BENCH throughput policy=${cell} " "${devlog}"; then dev_hit=1; fi
            else
                if grep -aqE "^BENCH throughput policy=${cell} .* dropped=[1-9]" "${devlog}"; then dev_hit=1; fi
            fi
            if [[ "${dev_hit}" == 1 && "${host_hit}" == 1 ]]; then
                grep -aE "^BENCH throughput policy=${cell} " "${devlog}" >>"${THRU_SAMPLES}" || true
                grep -E "^HOST throughput " "${log}" >>"${LOG_DIR}/host_${cell}.txt" || true
                valid_runs=$((valid_runs + 1))
                echo "=== ${cell} run ${run}/${RUNS}: one-way stream captured (device throughput + host delivered) ==="
            else
                echo "=== ${cell} run ${run}/${RUNS}: NO valid one-way capture (need device BENCH throughput + HOST throughput; device serial: ${devlog}) ==="
            fi
        elif grep -aq "BENCH resource policy=${cell}" "${devlog}"; then
            grep -aE "^BENCH sample policy=${cell} " "${devlog}" >>"${SAMPLES}" || true
            if [[ "${WORKLOAD}" == "pipeline" ]]; then
                grep -aE "^BENCH throughput policy=${cell} " "${devlog}" >>"${THRU_SAMPLES}" || true
            fi
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

# One-way aggregation. The HOST-delivered rate is the throughput headline (the design's authoritative
# number); the device accepted rate is the cross-check and the mean shed count the saturation witness.
# Host windows are attributed to a tier by their payload signature (bytes/msgs). Each HOST line is already a
# per-1s delivered rate, so the sustained rate is the MEDIAN over a tier's non-zero windows — this is
# immune to delivery draining past the fixed offer window (which would otherwise inflate a slow link's rate
# above its physical line rate, e.g. serial: summing over a ~7 s drain but dividing by the 3 s offer).
if [[ "${WORKLOAD}" == "oneway" ]]; then
    python3 - "${THRU_SAMPLES}" "${LOG_DIR}" "${CELLS[@]}" <<'PY'
import sys, re, os, glob, statistics

thru_path, log_dir = sys.argv[1], sys.argv[2]
cells = sys.argv[3:]
WIN = 3.0  # the per-tier offering window in seconds (k_oneway_window_us); the device cross-check divides by it

dev = {}   # (cell, tier) -> [(msgs, bytes, dropped)...]
with open(thru_path) as f:
    for line in f:
        m = re.match(r"BENCH throughput policy=(\S+) tier=(\d+) msgs=(\d+) bytes=(\d+) elapsed_us=(\d+) dropped=(\d+)", line)
        if m:
            dev.setdefault((m.group(1), int(m.group(2))), []).append((int(m.group(3)), int(m.group(4)), int(m.group(6))))

tiers = sorted({t for (_, t) in dev})
present = [c for c in cells if any((c, t) in dev for t in tiers)]

host = {}   # (cell, tier) -> [(msgs, bytes) per non-zero 1s window], attributed by payload signature
for c in present:
    hp = os.path.join(log_dir, f"host_{c}.txt")
    if not os.path.exists(hp):
        continue
    for line in open(hp, errors="ignore"):
        m = re.match(r"HOST throughput msgs=(\d+) bytes=(\d+) ", line)
        if m and int(m.group(1)) > 0 and tiers:
            msgs, byts = int(m.group(1)), int(m.group(2))
            t = min(tiers, key=lambda x: abs(x - byts / msgs))
            host.setdefault((c, t), []).append((msgs, byts))

heap = {}   # cell -> min free heap over its runs (the tightest headroom)
for c in present:
    vals = []
    for dl in glob.glob(os.path.join(log_dir, f"{c}_run_*_device.log")):
        for line in open(dl, "rb"):
            m = re.search(rb"BENCH resource policy=\S+ rx_stack_words=\d+ free_heap=(\d+)", line)
            if m:
                vals.append(int(m.group(1)))
    if vals:
        heap[c] = min(vals)

mean = lambda xs: sum(xs) / len(xs) if xs else 0.0

def hrate(c, t):
    ws = host.get((c, t), [])
    if not ws:
        return 0.0, 0.0
    return statistics.median(m for m, _ in ws), statistics.median(b for _, b in ws) / 1e6

def headline(title, idx, prec):
    print("\n" + title + "\n")
    print("| transport | " + " | ".join(f"{t} B" for t in tiers) + " |")
    print("| " + " | ".join(["---"] * (len(tiers) + 1)) + " |")
    best = {t: max((hrate(c, t)[idx] for c in present), default=0) for t in tiers}
    for c in present:
        row = [c]
        for t in tiers:
            v = hrate(c, t)[idx]
            row.append(f"**{v:.{prec}f}**" if abs(v - best[t]) < 10 ** -(prec + 1) else f"{v:.{prec}f}")
        print("| " + " | ".join(row) + " |")

headline("## One-way stream throughput — host-delivered msg/s (transport x payload)", 0, 0)
headline("## One-way stream throughput — host-delivered MB/s (transport x payload)", 1, 2)

for c in present:
    tag = f" (min free heap {heap[c]} B)" if c in heap else ""
    print(f"\n## {c} — host-delivered headline + device cross-check + shed witness{tag}\n")
    print("| payload | host msg/s | host MB/s | device msg/s | agreement | dropped (mean) | runs |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for t in tiers:
        rs = dev.get((c, t), [])
        if not rs:
            continue
        hr, hb = hrate(c, t)
        dr = mean([r[0] for r in rs]) / WIN
        dd = mean([r[2] for r in rs])
        agree = 100 * abs(dr - hr) / dr if dr else 0
        print(f"| {t} B | {hr:.0f} | {hb:.2f} | {dr:.0f} | {agree:.1f}% | {dd:.0f} | {len(rs)} |")

default = next((c for c in present if c == "lwip-p1"), None)
deep = next((c for c in present if c.endswith("-deep")), None)
if default and deep:
    print("\n**Widen cost — deep vs default egress cap (host-delivered msg/s):**\n")
    print("| payload | default (8 KiB) | deep | delta |")
    print("| --- | --- | --- | --- |")
    for t in tiers:
        a, b = hrate(default, t)[0], hrate(deep, t)[0]
        delta = 100 * (b - a) / a if a else 0
        print(f"| {t} B | {a:.0f} | {b:.0f} | {delta:+.1f}% |")
    if deep in heap:
        print(f"\nDeep-cap min free heap at sweep end: {heap[deep]} B — the deep cap fit alongside the Wi-Fi stack.")
PY
    echo "BENCH: all cells produced >=3 valid runs"
    exit 0
fi

# Pipeline aggregation. The device throughput lines give completed round trips over the sampling window at
# each depth K (round-trips/s); the per-completion sample lines give p50/p99 latency-at-depth. K=1 is
# sanity-checked against the shipped single-in-flight lwIP-P2 latency p50 — a >30% gap flags a drift.
if [[ "${WORKLOAD}" == "pipeline" ]]; then
    python3 - "${THRU_SAMPLES}" "${SAMPLES}" "${LOG_DIR}" <<'PY'
import sys, re

thru_path, samples_path, log_dir = sys.argv[1], sys.argv[2], sys.argv[3]

k_sampled = 128   # a healthy tier completes this many sampled round trips; fewer marks a stall-limited tier
rate = {}   # (K, tier) -> [round-trips/s ...] over the cell's valid runs
comp = {}   # (K, tier) -> [completed ...]; mean < k_sampled => the tier is buffer-bound at this depth
with open(thru_path, errors="ignore") as f:   # -a grep can splice device-log binary into a matched line
    for line in f:
        m = re.match(r"BENCH throughput policy=lwip-p2-k(\d+) tier=(\d+) window=(\d+) completed=(\d+) elapsed_us=(\d+)", line)
        if m:
            K, tier, completed, elapsed = int(m.group(1)), int(m.group(2)), int(m.group(4)), int(m.group(5))
            if elapsed > 0:
                rate.setdefault((K, tier), []).append(completed / (elapsed / 1e6))
                comp.setdefault((K, tier), []).append(completed)

rtt = {}    # (K, tier) -> [rtt_us ...]
with open(samples_path, errors="ignore") as f:
    for line in f:
        m = re.match(r"BENCH sample policy=lwip-p2-k(\d+) tier=(\d+) index=\d+ rtt_us=(-?\d+)", line)
        if m:
            rtt.setdefault((int(m.group(1)), int(m.group(2))), []).append(int(m.group(3)))

Ks    = sorted({k for (k, _) in rate} | {k for (k, _) in rtt})
tiers = sorted({t for (_, t) in rate} | {t for (_, t) in rtt})
mean  = lambda xs: sum(xs) / len(xs) if xs else 0.0

def pct(vals, q):
    s = sorted(vals)
    if not s:
        return None
    return s[max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))]

def stall_limited(k, t):
    cs = comp.get((k, t), [])
    return bool(cs) and mean(cs) < k_sampled

print("\n## Pipelined round-trips/s (window K x payload)\n")
print("| K | " + " | ".join(f"{t} B" for t in tiers) + " |")
print("| " + " | ".join(["---"] * (len(tiers) + 1)) + " |")
best = {t: max((mean(rate.get((k, t), [])) for k in Ks if not stall_limited(k, t)), default=0.0) for t in tiers}
for k in Ks:
    row = [str(k)]
    for t in tiers:
        vals = rate.get((k, t), [])
        if not vals:
            row.append("-")
        elif stall_limited(k, t):
            row.append(f"{mean(vals):.1f}†")
        else:
            v = mean(vals)
            row.append(f"**{v:.0f}**" if abs(v - best[t]) < 0.5 else f"{v:.0f}")
    print("| " + " | ".join(row) + " |")
print("\n† stall-limited: the tier did not complete its 128-sample window (completed < 128). The fixed RX slot pool cannot hold a deep window of this payload, so the figure is the stall-reissue floor, not a sustained pipeline rate.")

print("\n## Pipelined p50 round-trip latency-at-depth (us) (window K x payload)\n")
print("| K | " + " | ".join(f"{t} B" for t in tiers) + " |")
print("| " + " | ".join(["---"] * (len(tiers) + 1)) + " |")
for k in Ks:
    row = [str(k)]
    for t in tiers:
        v = pct(rtt.get((k, t), []), 0.50)
        row.append("-" if v is None else f"{v}")
    print("| " + " | ".join(row) + " |")

ref = {16: 7100, 256: 7900, 4096: 17800}   # shipped single-in-flight lwIP-P2 latency p50 (us)
print("\n## K=1 sanity — pipeline single-in-flight p50 vs the latency-bench p50\n")
print("| payload | K=1 pipeline p50 us | latency-bench p50 us | delta | verdict |")
print("| --- | --- | --- | --- | --- |")
diverged = False
for t in tiers:
    v = pct(rtt.get((1, t), []), 0.50)
    r = ref.get(t)
    if v is None or r is None:
        print(f"| {t} B | {'-' if v is None else v} | {r if r else '-'} | - | NO DATA |")
        continue
    d = 100.0 * (v - r) / r
    ok = abs(d) <= 30.0
    diverged = diverged or not ok
    print(f"| {t} B | {v} | {r} | {d:+.1f}% | {'SANITY OK' if ok else 'SANITY DIVERGENCE'} |")
if diverged:
    sys.stderr.write("BENCH: K=1 pipeline p50 DIVERGES >30% from the latency-bench p50 — the pipeline path may have drifted from the single-in-flight baseline\n")
PY
    echo "BENCH: all cells produced >=3 valid runs"
    exit 0
fi

# Aggregate p50/p99 per (cell x tier) and emit the standing suite table format: a plexus-only
# transport x payload table, then one table per transport type. p50/p99 are nearest-rank order
# statistics over every sampled round trip across all valid runs of the cell.
python3 - "${SAMPLES}" "${LOG_DIR}" <<'PY'
import sys, glob, os, re, statistics

samples_path, log_dir = sys.argv[1], sys.argv[2]

rtt = {}    # (cell, tier) -> [rtt_us...]
with open(samples_path, errors="ignore") as f:
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
