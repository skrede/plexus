# Examples

## Configure and build

```sh
cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_MDNSPP_DISCOVERY=ON
cmake --build build -j4
```

The in-process example needs only the default configuration; the asio + mDNS examples
need the two backend options above.

## Run

In-process zero-serialization fast path (single binary):

```sh
./build/examples/typed_inproc_fastpath
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
