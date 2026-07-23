# Fuzz + sanitizer gate

This directory holds the libFuzzer harnesses that exercise the untrusted-input
decode surface, the local regression corpus they replay, and the protocol
for running the repeatable hardening gate (fuzz replay + a bounded campaign, plus
the sanitizer trees the gate depends on). The gate is deterministic-replay-first:
a local corpus is replayed when present, and a short bounded campaign hunts
for fresh crashes within a fixed time budget.

## Harnesses

| Harness | Surface |
|---------|---------|
| `fuzz_data_frame` | `wire::decode_unidirectional` / `decode_bidirectional` |
| `fuzz_rpc_frame` | the RPC frame decoder |
| `fuzz_handshake_frame` | the handshake frame decoder |
| `fuzz_frame_reassembler` | the wire-level frame reassembler decode path |
| `fuzz_reassembler` | `wire::decode_udp_fragment_header` + the bounded datagram `reassembler::feed` |

Each harness is fail-closed by construction: every field is range-checked before
it indexes, and a fresh decoder/reassembler instance is built per input so no
partial-message state crosses inputs.

## Corpus

`corpus/<harness>/` is a local, git-ignored regression corpus — a build/run
artifact, never committed (binary corpora do not live in this repo). Treat it as
the minimal coverage-covering set produced by `-merge=1`: the three wire-frame
decoders collapse to a single coverage edge so one seed suffices; the two
reassembler harnesses carry the coverage-distinct fragment-header /
fragment-index / fragment-count combinations the range checks must survive. Keep
it minimal — re-minimize after any growth (see below). Because the corpus is not
checked in, replay is a no-op on a fresh checkout until a campaign has populated
it; the bounded campaign below reaches full decoder coverage from an empty corpus
in seconds, so a clean run needs no seed. Durable, shared corpus storage is a
separate, still-open question (see the decision seed in `.planning/seeds/`).

## Building the fuzz tree

libFuzzer pairs with AddressSanitizer and requires a Clang toolchain. The harness
targets compile only when the compiler is Clang (the CMake subtree skips with a
warning otherwise) and are gated behind `PLEXUS_BUILD_FUZZ_TESTS`:

```bash
cmake -S . -B build-fuzz -DCMAKE_CXX_COMPILER=clang++ -DPLEXUS_BUILD_FUZZ_TESTS=ON
cmake --build build-fuzz -j4
```

Each target is built with `-fsanitize=fuzzer,address -fno-sanitize-recover=all`.
Keep this tree separate from the thread-sanitizer tree below: libFuzzer cannot
combine with ThreadSanitizer (it pairs with AddressSanitizer only).

## The repeatable gate

### (a) Deterministic corpus replay — the gate

Replays the committed corpus with no new mutation (`-runs=0`); must exit 0 for
every harness.

```bash
for h in fuzz_data_frame fuzz_rpc_frame fuzz_handshake_frame fuzz_frame_reassembler fuzz_reassembler; do
    build-fuzz/tests/fuzz/$h tests/fuzz/corpus/$h -runs=0 || echo "REPLAY FAIL $h"
done
```

### (b) Bounded fresh campaign

A time-bounded mutation campaign per harness; any crash, sanitizer report, or
non-zero exit fails the gate. Sixty seconds per harness keeps the full gate under
a few minutes.

```bash
for h in fuzz_data_frame fuzz_rpc_frame fuzz_handshake_frame fuzz_frame_reassembler fuzz_reassembler; do
    build-fuzz/tests/fuzz/$h -max_total_time=60 -timeout=10 tests/fuzz/corpus/$h || echo "CAMPAIGN FAIL $h"
done
```

### Re-minimizing after a campaign grows the corpus

A campaign appends coverage-new inputs to `corpus/<harness>/`. Before committing,
re-minimize so the regression corpus stays the minimal coverage-covering set:

```bash
tmp=$(mktemp -d); mv tests/fuzz/corpus/$h "$tmp/src"; mkdir -p tests/fuzz/corpus/$h
build-fuzz/tests/fuzz/$h -merge=1 tests/fuzz/corpus/$h "$tmp/src"; rm -rf "$tmp"
```

## The sanitizer trees

The fuzz gate is the untrusted-input leg of a broader sanitizer matrix. The two
companion trees:

### AddressSanitizer + UndefinedBehaviorSanitizer (all backends)

```bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=all -g" \
    -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_CRYPTO_BACKEND=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON
cmake --build build-asan -j4
ctest --test-dir build-asan -j4 --timeout 120
```

`-fno-sanitize-recover=all` makes any UB the first report fatal. Zero reports is
the pass bar; the datagram reassembler (the untrusted-input surface) is included.

### ThreadSanitizer (full transport matrix)

ThreadSanitizer must cover every transport, not just the shared-memory ring — the
backend matrix mirrors the AddressSanitizer tree, only the sanitizer flag differs.
The genuinely-threaded surface is the lock-free shared-memory ring plus the
cross-process notifier; the transport data paths are single-io-thread by design,
so ThreadSanitizer over them is a no-accidental-sharing assurance.

```bash
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer -g" \
    -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_CRYPTO_BACKEND=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON
cmake --build build-tsan -j4
ctest --test-dir build-tsan -j4 --timeout 120
```

Do not add `,address` or `,undefined` to the ThreadSanitizer flags — it does not
combine with AddressSanitizer. A few `-Wtsan` warnings from asio's fenced-block
(`atomic_thread_fence` is uninstrumentable under ThreadSanitizer) are benign and
inherent to running asio under ThreadSanitizer; they are not data races.

### Cross-process notifier stress

The cross-process shared-memory notifier / teardown path is the highest-value
race target. A targeted high-iteration run under heavy contention discriminates a
deadline slip (a timing artifact under load — completes, just slow) from a
lost-wakeup hang (the notifier word is signaled before the consumer parks). A run
that completes within the per-test timeout is hang-free; a run that hits the
timeout without progress is a lost-wakeup and a release blocker, not a flake.

```bash
for i in $(seq 1 50); do
    ctest --test-dir build -j16 --timeout 60 -R 'shm\.(notifier|teardown)' --output-on-failure \
        || echo "ITER $i NONZERO"
done
```
