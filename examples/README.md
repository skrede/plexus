# Examples

The public face of the API. Each example is one terminal binary, grouped by the pattern it
demonstrates:

| Folder                       | What it shows                                                        |
| ---------------------------- | -------------------------------------------------------------------- |
| [`discovery/`](discovery)    | Zero-config discovery out of the box, plus universes + liveliness    |
| [`pub_sub/`](pub_sub)        | Publish/subscribe, in typed and raw-bytes flavors (asio + mDNS)      |
| [`req_res/`](req_res)        | Request/response, in typed and raw-bytes flavors (asio + mDNS)       |
| [`inproc/`](inproc)          | The in-process zero-serialization fast path                          |
| [`logging/`](logging)        | Projecting a typed topic to CSV via `node.log<Codec, Projection>`    |
| [`recording/`](recording)    | Capturing a session to a flat stream, and transcoding it to MCAP     |
| [`same_host/`](same_host)    | Same-host nodes over shm + AF_UNIX + TCP                             |
| [`qos/`](qos)                | The size-envelope QoS knob                                           |
| [`mcu/`](mcu)                | plexus on an ESP32 over a real UART — see [mcu/README.md](mcu)       |

## Configure and build

```sh
cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_MDNSPP_DISCOVERY=ON
cmake --build build -j4
```

The in-process examples (`inproc/`, `logging/`, `recording/`) need only the default
configuration; the networked examples (`discovery/`, `pub_sub/`, `req_res/`, `qos/`) need the asio backend
(and mDNS for `pub_sub`/`req_res`). Binaries land in `build/examples/<folder>/<name>`.

## Single-binary examples

These run and exit on their own — no second peer, no mDNS.

In-process zero-serialization fast path:

```sh
./build/examples/inproc/typed_inproc_fastpath
```

Logging a typed topic to CSV on stdout (`node.log<Codec, Projection>`): a projection names the
columns and emits field values per sample; a type without a projection logs through the
`operator<<` text floor.

```sh
./build/examples/logging/logging_example
```

Recording a multi-endpoint session to a flat record stream (a recording QoS +
`node.make_recorder` over a consumer-owned `byte_sink`). It captures the session, prints the
captured byte count, and writes the flat stream to `recording_example_capture.plxr`:

```sh
./build/examples/recording/recording_example
```

To turn the captured flat stream into an MCAP container Foxglove can open, either build with the
transcode (`-DPLEXUS_BUILD_MCAP_TRANSCODE=ON`, which converts in-process) or run the standalone
transcode on the written file:

```sh
cmake --build build --target mcap_transcode   # needs -DPLEXUS_BUILD_MCAP_TRANSCODE=ON
./build/tools/mcap_transcode/mcap_transcode recording_example_capture.plxr out.mcap
```

Two end-to-end MCAP examples — each captures a session and converts it to MCAP in one run (built
only with `-DPLEXUS_BUILD_MCAP_TRANSCODE=ON`). Each writes a `.plxr` + a `.mcap` and prints the
transcode summary; the file headers document how to open the output in Foxglove
(https://studio.foxglove.dev) or inspect it with the `mcap` CLI (`mcap info <file>`).

`mcap_basic_foxglove` — the codec emits a small JSON object per sample and the recorder declares
a matching jsonschema in the stream preamble, so the data channels decode and plot in Foxglove
out of the box:

```sh
cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_BUILD_MCAP_TRANSCODE=ON
cmake --build build -j4 --target mcap_basic_foxglove
./build/examples/recording/mcap_basic_foxglove
```

`mcap_opaque_supplied_schema` — the payload is opaque to plexus's store (a finished byte blob)
and the producer SUPPLIES a truthful encoding + schema the bytes actually satisfy. It shows the
explicit supplied-schema mechanism; the header states the honesty caveat (never declare an
encoding the bytes do not satisfy — the transcode validates nothing):

```sh
cmake --build build -j4 --target mcap_opaque_supplied_schema
./build/examples/recording/mcap_opaque_supplied_schema
```

## Same-host nodes (Linux + shm)

Both stand up a same-host listener and exit rc=0; the same-host SHM delivery data path is proven
by the `shm.` test suite. They are Linux-only and need `-DPLEXUS_ENABLE_SHM_BACKEND=ON` and
liburing.

```sh
cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON
cmake --build build -j4 --target same_host_node --target shm_local_node
```

`same_host_node` — ONE consumer surface with no platform conditional in the consumer's code.
`plexus::asio::same_host_transports` resolves, behind its header, to the most accelerated
same-host substrate the host offers (shm + AF_UNIX + TCP on Linux, AF_UNIX + TCP elsewhere). Its
optional `region` argument is the shm-region namespace: empty (the default) lets two peers share
an shm ring by topic, while a distinct `region` per application isolates its shared memory so two
unrelated co-host apps using the same topic names never collide.

```sh
./build/examples/same_host/same_host_node
```

`shm_local_node` — a live node naming shared memory as a same-host transport leaf EXPLICITLY. It
names shm + AF_UNIX + plain TCP in a `transport_set`, which owns the leaves and mints the node.
The file header documents the alternatives (the hand-built leaf pack, the `make_shm_member`
recipe, the lean crypto-free `local_shm_mux` alias).

```sh
./build/examples/same_host/shm_local_node
```

## Two-terminal examples

Each runs the first binary in one shell and the second in another. Discovery over mDNS takes a
few seconds to converge.

Zero-config discovery — one binary run twice (pass `second` to the second instance). Each node
composes `plexus::asio::default_discovery` with no group, port, or interface configuration and
prints when it has noted its peer; the same-host case works over multicast loopback out of the
box:

```sh
./build/examples/discovery/discovery_hello           # shell 1
./build/examples/discovery/discovery_hello second    # shell 2
```

The two knobs on top of the same composition: a universe assignment via `discovery_options`
(nodes in another universe never rendezvous with these) and a liveliness observer printing the
fused peer-alive / peer-lost verdicts. Start both, then kill one — the survivor prints the lost
verdict once the peer's awareness ages out:

```sh
./build/examples/discovery/discovery_fleet           # shell 1
./build/examples/discovery/discovery_fleet second    # shell 2
```

Pub/sub (bytes, then typed):

```sh
./build/examples/pub_sub/bytes_pubsub_pub      # shell 1
./build/examples/pub_sub/bytes_pubsub_sub      # shell 2

./build/examples/pub_sub/typed_pubsub_pub      # shell 1
./build/examples/pub_sub/typed_pubsub_sub      # shell 2
```

Request/response (bytes, then typed):

```sh
./build/examples/req_res/bytes_reqres_server   # shell 1
./build/examples/req_res/bytes_reqres_client   # shell 2

./build/examples/req_res/typed_reqres_server   # shell 1
./build/examples/req_res/typed_reqres_client   # shell 2
```

## MCU

The ESP32-over-UART example has its own build and flash flow — see [mcu/README.md](mcu).
