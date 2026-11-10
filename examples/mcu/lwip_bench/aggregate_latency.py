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

# Order statistics at the rounded index q*(n-1), over every sampled round trip of every valid run of the cell.
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
