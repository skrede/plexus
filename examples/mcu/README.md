# MCU example — plexus on an ESP32 over a real UART

A full plexus node running on a constrained target. The board ([`esp-idf/`](esp-idf/)) stands
up the cooperative executor, a real UART0 `byte_channel`, a node, and a 1 Hz timer that samples
the BOOT button (GPIO0) and publishes the reading on the `telemetry` topic through the real
handshake + framing + CRC + pub/sub engine. The same code that runs on the desktop runs here —
only the executor, timer, and channel are the FreeRTOS/UART substrate.

The host half ([`serial_gate_host.cpp`](serial_gate_host.cpp)) dials the board over the serial
link, completes the handshake, subscribes, and asserts one real message arrives. It reuses the
desktop `serial_transport` verbatim — no host-side transport code is written for the board.

## What you need

- A C-series or classic **ESP32 dev board** (the example cross-builds for `esp32` (Xtensa) and
  `esp32c3` (RISC-V)) on a USB cable that exposes the auto-reset lines (most CP2102/CH340 boards).
- **ESP-IDF v6.x** installed. On this machine it lives at `/opt/esp-idf`; elsewhere, follow
  Espressif's [get-started guide](https://docs.espressif.com/projects/esp-idf/en/stable/esp32/get-started/).

The console is disabled (`CONFIG_ESP_CONSOLE_NONE`) because plexus owns UART0 — so **do not run
`idf.py monitor`**; it would hold the port the link needs, and there is no console output anyway.

## Flash it manually

```sh
. /opt/esp-idf/export.sh            # bring idf.py + the cross-toolchain into scope
cd examples/mcu/esp-idf

idf.py set-target esp32             # or: idf.py set-target esp32c3
idf.py build
idf.py -p /dev/ttyUSB0 flash        # adjust the port to your board
```

`set-target` only needs re-running when you switch chips. After `flash` the board resets into
the app and begins publishing; nothing is printed on the wire except framed plexus bytes.

## See a message arrive on the host

Build the host gate (asio backend) from the repo root and run it against the same port:

```sh
cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_ENABLE_ASIO_BACKEND=ON
cmake --build build -j4 --target serial_gate_host
./build/examples/mcu/serial_gate_host           # dials /dev/ttyUSB0@115200, asserts one message
```

It prints `GATE_PASS value=<v>` and exits 0 on receipt, or `GATE_FAIL` and non-zero on a
few-second timeout.

For a reproducible end-to-end check, [`run_serial_gate.sh`](run_serial_gate.sh) cross-builds the
firmware, then loops flash + assert at least three times (a single pass never substantiates a
link claim) and fails if any iteration misses the message:

```sh
./examples/mcu/run_serial_gate.sh [RUNS] [PORT]   # defaults: 3 runs, /dev/ttyUSB0
```

# MCU example — plexus on an ESP32 over TCP/IP

Two firmware variants run the same node over a real IP transport instead of the UART:

- [`esp-idf-lwip/`](esp-idf-lwip/) — **Wi-Fi STA**. The board joins your access point, takes a
  DHCP lease, and dials a fixed host endpoint over TCP, publishing the `telemetry` topic each
  second. This is the on-hardware gate: the host [`lwip_gate_host.cpp`](lwip_gate_host.cpp)
  listens, accepts the device's inbound dial, and asserts one real message arrives.
- [`esp-idf-eth/`](esp-idf-eth/) — **Ethernet**. The same node over a wired PHY. It cross-builds
  for `esp32`, but no on-hardware run is claimed until a PHY board is available — treat it as a
  compile-only target for now.

Unlike the serial example the device is the **dialer** here: it brings up the netif, then dials
the host. The host gate is therefore a TCP **server** (`node.listen`), not a dialer.

## Wi-Fi credentials (operator-supplied, never committed)

The SSID and password live in `esp-idf-lwip/main/wifi_credentials.h`, which is gitignored. Create
it from the template and fill in the real values:

```sh
cp examples/mcu/esp-idf-lwip/main/wifi_credentials.h.example \
   examples/mcu/esp-idf-lwip/main/wifi_credentials.h
# edit it: set WIFI_SSID and WIFI_PASSWORD to your network's values
```

Never commit this file. No tooling reads or echoes its contents.

## The host endpoint

The firmware dials `192.168.1.69:7447` by default (`PLEXUS_HOST_ENDPOINT`) — a private LAN
address, not a secret. Override it at build time if your host has a different address, and keep
the port aligned with the host server's listen port (default `7447`).

## Flash the Wi-Fi firmware manually

```sh
. /opt/esp-idf/export.sh
cd examples/mcu/esp-idf-lwip

idf.py set-target esp32
idf.py build
idf.py -p /dev/ttyUSB0 flash        # adjust the port to your board
```

## The on-hardware gate

Build the host server, then run the reproducible gate from the repo root:

```sh
cmake --build build -j4 --target lwip_gate_host
./examples/mcu/run_lwip_gate.sh [RUNS] [PORT] [HOST_PORT]   # defaults: 3, /dev/ttyUSB0, 7447
```

The runner cross-builds the Wi-Fi firmware (a green cross-compile is **not** a hardware proof on
its own), then per run starts the host server, flashes the board, drives the auto-reset lines into
RUN, captures the device serial log under `build/examples/mcu/lwip_gate_logs/`, and asserts the
host received a real message over TCP. It requires at least three reproducible passes — any miss
fails the whole gate. The on-hardware run needs the operator-supplied credentials above; it cannot
run without them.

# MCU bench — request/echo round trip over serial, lwIP-P1, and lwIP-P2

[`esp-idf-lwip-bench/`](esp-idf-lwip-bench/) is the on-device bench firmware. It reuses the Wi-Fi
bring-up and dial path of the Wi-Fi example, but swaps the publish loop for a **request/echo
round-trip** workload: for each payload tier the device publishes a tier-sized request on the
`request` topic, the host echoes it verbatim on `reply`, and the device times the round trip with
the microsecond `esp_timer` clock. It samples the round trip across the tiers and reports the
RX-task stack high-water plus the free heap, emitting one machine-parseable line per sample on the
console (UART0) for the runner to parse.

The same firmware runs over three transports, selected at build time (`BENCH_TRANSPORT`):

- **serial** — the plexus link rides UART1 (GPIO17 TX / GPIO16 RX) so the console stays on UART0
  for the sample lines. This is the serial path's first MCU bench. It is opt-in (it needs a second
  USB-serial adapter on the UART1 pads and the echo peer wired to it).
- **lwIP-P1** — the poll-drive receive policy (thread-free; the super-loop drains a non-blocking
  recv).
- **lwIP-P2** — the RX-task receive policy (a dedicated task blocking-recvs and posts the feed to
  the executor task).

The bench measures the **MCU-appropriate** lwIP config: the small 5760 B window is kept (it is NOT
raised for throughput — a tuned-up window would inflate the numbers). The payload tiers are
**16 / 256 / 4096 B**, bounded by the per-message ceiling; the large tier stays under one advertised
window.

## What the bench substantiates

The P1 (thread-free) versus P2 (lower-latency) receive policy is a **consumer-sovereign tradeoff**.
The bench produces the side-by-side p50/p99 round-trip table plus P2's incremental RAM/stack cost so
the choice is made on evidence. It does **not** pre-pick a winner: both policies are run and
reported, and neither is removed.

## Run the bench

The bench reuses the Wi-Fi credentials step and the host-endpoint repoint above. Build the host echo
peer, then run the reproducible runner from the repo root:

```sh
cmake --build build -j4 --target lwip_bench_host
PLEXUS_HOST_ENDPOINT=<host-ip:port> ./examples/mcu/run_lwip_bench.sh [RUNS] [PORT] [HOST_PORT]
# defaults: 3 runs/cell, /dev/ttyUSB0, 7447 ; add BENCH_SERIAL=1 for the serial cell
```

For each `{lwIP-P1, lwIP-P2}` cell (and `serial` when opted in) it cross-builds the firmware for
`esp32`, flashes, drives the auto-reset lines into RUN, captures the device console under
`build/examples/mcu/lwip_bench_logs/`, and parses the sample lines. It requires **at least three
valid runs per cell** — a single lucky pass never substantiates a number — and fails the whole bench
if any cell falls short. It prints the standing suite-format table: a transport x payload latency
table, then a per-transport p50/p99 + RX-task RAM/stack breakdown.

## Editors and IDEs

- **VS Code** — the [Espressif ESP-IDF extension](https://github.com/espressif/vscode-esp-idf-extension)
  is the most polished path: it wraps configure/build/flash/monitor and the `idf.py` flow this
  example already uses.
- **CLion** — two options:
  - the official [ESP-IDF plugin](https://plugins.jetbrains.com/plugin/23886-esp-idf)
    ([source](https://github.com/espressif/idf-clion-plugin)) creates and configures ESP-IDF
    CMake projects (still beta — some manual IDF configuration);
  - CLion's [built-in ESP-IDF support](https://www.jetbrains.com/help/clion/esp-idf.html), with
    Espressif's [CLion guide](https://docs.espressif.com/projects/esp-idf/en/stable/esp32/third-party-tools/clion.html)
    walking through configure/build/flash/debug over the CMake toolchain + OpenOCD.

  Point the IDE at [`esp-idf/`](esp-idf/) (the project root holding the top-level
  `CMakeLists.txt`), not this directory.
