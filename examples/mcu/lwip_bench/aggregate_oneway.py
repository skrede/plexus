import sys, re, os, glob, statistics

thru_path, log_dir = sys.argv[1], sys.argv[2]
cells = sys.argv[3:]
# The per-tier offering window in seconds (k_oneway_window_us); the device cross-check divides by it.
WIN = 3.0

dev = {}   # (cell, tier) -> [(msgs, bytes, dropped)...]
with open(thru_path) as f:
    for line in f:
        m = re.match(r"BENCH throughput policy=(\S+) tier=(\d+) msgs=(\d+) bytes=(\d+) elapsed_us=(\d+) dropped=(\d+)", line)
        if m:
            dev.setdefault((m.group(1), int(m.group(2))), []).append((int(m.group(3)), int(m.group(4)), int(m.group(6))))

tiers = sorted({t for (_, t) in dev})
present = [c for c in cells if any((c, t) in dev for t in tiers)]

# A host window is attributed to the tier nearest its payload signature (bytes/msgs).
host = {}   # (cell, tier) -> [(msgs, bytes) per non-zero 1s window]
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

heap = {}   # cell -> min free heap over its runs
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

# The host-delivered rate is the headline, the device accepted rate the cross-check. Each HOST line is a per-1s rate,
# so the median over a tier's non-zero windows is immune to delivery draining past the fixed offer window, which
# would inflate a slow link above its line rate (serial: a ~7 s drain divided by the 3 s offer).
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
