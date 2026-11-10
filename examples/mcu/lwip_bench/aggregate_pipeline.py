import sys, re

thru_path, samples_path, log_dir = sys.argv[1], sys.argv[2], sys.argv[3]

# A healthy tier completes this many sampled round trips; a mean below it marks the tier buffer-bound at that depth.
k_sampled = 128
rate = {}   # (K, tier) -> [round-trips/s ...]
comp = {}   # (K, tier) -> [completed ...]
# grep -a can splice device-log binary into a matched line.
with open(thru_path, errors="ignore") as f:
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

# The shipped single-in-flight lwIP-P2 latency p50 (us); a K=1 pipeline p50 more than 30% off it flags a drift.
ref = {16: 7100, 256: 7900, 4096: 17800}
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
