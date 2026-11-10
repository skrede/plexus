# Sourced by run_lwip_bench.sh before the ESP-IDF export: it selects CELLS for the workload and exits the runner on
# a refusal, so a refused run never reaches the export or a build.

WORKLOAD="${BENCH_WORKLOAD:-latency}"

CELLS=(lwip-p1 lwip-p2)
if [[ "${BENCH_SERIAL_ONLY:-0}" == "1" ]]; then
    CELLS=(serial)
elif [[ "${BENCH_SERIAL:-0}" == "1" ]]; then
    CELLS=(serial lwip-p1 lwip-p2)
fi

# The one-way stream is device->host only (no ingress), so it uses the poll-drive P1 policy: no RX task to idle or
# crash. The default 8 KiB egress cap is the honest headline; the deep cell shows the widen cost beside it.
if [[ "${WORKLOAD}" == "oneway" && "${BENCH_SERIAL_ONLY:-0}" != "1" ]]; then
    CELLS=(lwip-p1)
    [[ "${BENCH_DEEP_EGRESS:-0}" == "1" ]] && CELLS=(lwip-p1 lwip-p1-deep)
fi

# Request/reply has ingress, so the window-depth sweep needs the RX task (lwIP-P2); each K is a full rebuild so
# BENCH_WINDOW_K is compiled in. K=1 reduces to latency.
if [[ "${WORKLOAD}" == "pipeline" && "${BENCH_SERIAL_ONLY:-0}" != "1" ]]; then
    CELLS=()
    for k in 1 2 4 8 16; do CELLS+=("lwip-p2-k${k}"); done
fi

# The baud rides the cell name so the per-transport table is a baud x payload map; the high FTDI rates settle
# whether the link holds without RTS/CTS flow control.
if [[ "${WORKLOAD}" == "oneway" && "${BENCH_SERIAL_ONLY:-0}" == "1" ]]; then
    CELLS=()
    for b in 115200 460800 921600; do CELLS+=("serial-b${b}"); done
fi

# There is no serial-k path: a serial pipeline would build a mislabeled cell whose samples the aggregator drops.
if [[ "${WORKLOAD}" == "pipeline" && "${BENCH_SERIAL_ONLY:-0}" == "1" ]]; then
    echo "serial pipeline is not supported by this runner; run the pipeline workload over Wi-Fi (lwip)."
    exit 2
fi

has_cell() {
    local c
    for c in "${CELLS[@]}"; do [[ "${c}" == "$1" ]] && return 0; done
    return 1
}

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

derive_build_defines() {
    build_transport="$1"
    deep_def=""
    window_def=""
    baud_def=""
    if [[ "$1" == "lwip-p1-deep" ]]; then
        build_transport="lwip-p1"
        deep_def="-DBENCH_DEEP_EGRESS=1"
    elif [[ "$1" =~ ^lwip-p2-k([0-9]+)$ ]]; then
        build_transport="lwip-p2"
        window_def="-DBENCH_WINDOW_K=${BASH_REMATCH[1]}"
    elif [[ "$1" =~ ^serial-b([0-9]+)$ ]]; then
        build_transport="serial"
        baud_def="-DBENCH_BAUD=${BASH_REMATCH[1]}"
    fi
}
