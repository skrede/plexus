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
