# Reset the board into RUN through the dev-board auto-reset circuit (EN/IO0 on the adapter's RTS/DTR): IO0 high (run,
# not the download ROM), then pulse EN low->high. The same open captures the console: a second open would reset again.
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

# esptool's DTR/RTS reset into the bootloader is flaky on this adapter ("chip stopped responding", about 1 in 3), so
# a stuck reset is retried instead of aborting the sweep.
flash_board() {
    local cell="$1" run="$2" log="$3" attempt
    for attempt in 1 2 3 4 5; do
        if idf.py -C "${IDF_PROJECT}" -B "${IDF_PROJECT}/build_esp32" -p "${PORT}" flash >>"${log}" 2>&1; then
            return 0
        fi
        echo "=== ${cell} run ${run}/${RUNS}: flash attempt ${attempt} failed; settling then retrying ==="
        sleep 3
    done
    echo "=== ${cell} run ${run}/${RUNS}: flash failed after retries (board not entering download mode) ==="
    sleep 3
    return 1
}

# The validators count a valid run in the caller's valid_runs. A valid one-way run needs device throughput lines and
# host-delivered lines; lwIP also needs a nonzero shed (dropped>0) as its saturation witness, but serial has no
# congestion shed (its dropped counts CRC-RX errors).
validate_oneway_run() {
    local cell="$1" run="$2" log="$3" devlog="$4" host_hit=0 dev_hit=0
    if grep -q "^HOST throughput " "${log}"; then host_hit=1; fi
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
}

# The device prints its BENCH resource line once it has swept every tier, so the line marks a complete run.
validate_sweep_run() {
    local cell="$1" run="$2" devlog="$3"
    if grep -aq "BENCH resource policy=${cell}" "${devlog}"; then
        grep -aE "^BENCH sample policy=${cell} " "${devlog}" >>"${SAMPLES}" || true
        if [[ "${WORKLOAD}" == "pipeline" ]]; then
            grep -aE "^BENCH throughput policy=${cell} " "${devlog}" >>"${THRU_SAMPLES}" || true
        fi
        valid_runs=$((valid_runs + 1))
        echo "=== ${cell} run ${run}/${RUNS}: complete sweep captured ==="
    else
        echo "=== ${cell} run ${run}/${RUNS}: NO complete sweep (device serial: ${devlog}) ==="
    fi
}
