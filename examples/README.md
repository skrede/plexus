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
