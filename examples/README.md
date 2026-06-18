# Examples

## Configure and build

```sh
cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_MDNSPP_DISCOVERY=ON
cmake --build build -j4
```

The in-process example needs only the default configuration; the asio + mDNS examples
need the two backend options above.

## Run

The single-binary in-process examples below need only the default configuration
(`-DPLEXUS_BUILD_EXAMPLES=ON`); they run and exit on their own, no backends or mDNS.

In-process zero-serialization fast path:

```sh
./build/examples/typed_inproc_fastpath
```

Logging a typed topic to CSV on stdout (`node.log<Codec, Projection>`): a projection names
the columns and emits field values per sample; a type without a projection logs through the
`operator<<` text floor.

```sh
./build/examples/logging_example
```

Recording a multi-endpoint session to a flat record stream (a recording QoS +
`node.make_recorder` over a consumer-owned `byte_sink`). It captures the session, prints the
captured byte count, and writes the flat stream to `recording_example_capture.plxr`:

```sh
./build/examples/recording_example
```

To turn the captured flat stream into an MCAP container Foxglove can open, either build the
example with the transcode (`-DPLEXUS_BUILD_MCAP_TRANSCODE=ON`, which converts in-process)
or run the standalone transcode on the written file:

```sh
cmake --build build --target mcap_transcode   # needs -DPLEXUS_BUILD_MCAP_TRANSCODE=ON
./build/tools/mcap_transcode/mcap_transcode recording_example_capture.plxr out.mcap
```

Two end-to-end MCAP examples — each captures a session and converts it to MCAP in one run
(built only with `-DPLEXUS_BUILD_MCAP_TRANSCODE=ON`). Each writes a `.plxr` + a `.mcap` and
prints the transcode summary; the file headers document how to open the output in Foxglove
(https://studio.foxglove.dev) or inspect it with the `mcap` CLI (`mcap info <file>`).

`mcap_basic_foxglove` — the codec emits a small JSON object per sample and the recorder
declares a matching jsonschema in the stream preamble, so the data channels decode and plot in
Foxglove out of the box:

```sh
cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_BUILD_MCAP_TRANSCODE=ON
cmake --build build -j4 --target mcap_basic_foxglove
./build/examples/mcap_basic_foxglove
```

`mcap_opaque_supplied_schema` — the payload is opaque to plexus's store (a finished byte blob)
and the producer SUPPLIES a truthful encoding + schema the bytes actually satisfy. It shows the
explicit supplied-schema mechanism; the header states the honesty caveat (never declare an
encoding the bytes do not satisfy — the transcode validates nothing):

```sh
cmake --build build -j4 --target mcap_opaque_supplied_schema
./build/examples/mcap_opaque_supplied_schema
```

The portable same-host node: ONE consumer surface that works on every platform with no
platform conditional in the consumer's code. `plexus::asio::same_host_transports` resolves,
behind its header, to the most accelerated same-host substrate the host offers — shm +
AF_UNIX + TCP on Linux, AF_UNIX + TCP elsewhere — and mints a node held via `auto` and driven
through the identical node public API. Build it as below (on Linux the accelerated leaf needs
`-DPLEXUS_ENABLE_SHM_BACKEND=ON` and liburing); it stands up a same-host listener and exits
rc=0. The same-host SHM delivery data path is proven by the `shm.` test suite:

```sh
cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON
cmake --build build -j4 --target same_host_node
./build/examples/same_host_node
```

A live node naming shared memory as a same-host transport leaf EXPLICITLY (Linux-only; needs
`-DPLEXUS_ENABLE_SHM_BACKEND=ON` and liburing). It names shm + AF_UNIX + plain TCP in a
`transport_set`, which owns the leaves and mints the node (`ts.make_node<asio_policy>(...)`),
brings up a same-host listener, and exits rc=0. The file header documents the alternatives —
the hand-built leaf pack, the `make_shm_member` recipe, and the lean crypto-free
`local_shm_mux` alias; the same-host SHM delivery data path is proven by the `shm.` test suite:

```sh
cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON
cmake --build build -j4 --target shm_local_node
./build/examples/shm_local_node
```

Each two-terminal example below runs the first binary in one shell and the second in
another. Discovery over mDNS takes a few seconds to converge.

Bytes pub/sub:

```sh
./build/examples/bytes_pubsub_pub      # shell 1
./build/examples/bytes_pubsub_sub      # shell 2
```

Bytes request/response:

```sh
./build/examples/bytes_reqres_server   # shell 1
./build/examples/bytes_reqres_client   # shell 2
```

Typed pub/sub:

```sh
./build/examples/typed_pubsub_pub      # shell 1
./build/examples/typed_pubsub_sub      # shell 2
```

Typed request/response:

```sh
./build/examples/typed_reqres_server   # shell 1
./build/examples/typed_reqres_client   # shell 2
```
