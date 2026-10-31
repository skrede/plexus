# Fuzz + sanitizer gate

This directory holds the libFuzzer harnesses over the untrusted-input surface, the seed emitter that generates their
starting corpus from the real encoders, and the replay test that runs every seed on each build. CI fuzzes every harness
under a time budget against an accumulated corpus kept in a separate store. The sanitizer trees the gate depends on are
described at the end.

## Harnesses

| Harness | Surface |
|---------|---------|
| `fuzz_data_frame` | `wire::decode_unidirectional` / `decode_bidirectional`, and `wire::framed_sequence` checked against the unidirectional decode |
| `fuzz_rpc_frame` | `wire::decode_rpc_request` / `decode_rpc_response` |
| `fuzz_handshake_frame` | `wire::decode_handshake_request` / `decode_handshake_response` |
| `fuzz_subscribe_frame` | `wire::decode_subscribe_request` / `decode_subscribe_response` |
| `fuzz_fetch_latched_frame` | `wire::decode_fetch_latched_request` |
| `fuzz_heartbeat_frame` | `wire::decode_heartbeat` |
| `fuzz_topic_declaration_frame` | `wire::decode_topic_declaration` |
| `fuzz_announcement_frame` | `wire::decode_announcement` |
| `fuzz_peer_report_frame` | `wire::decode_peer_report` |
| `fuzz_forwarded_frame` | `wire::decode_forwarded_frame` |
| `fuzz_companion_frame` | `wire::decode_companion` / `decode_companion_ready` / `decode_companion_switch`, each accepted payload re-encoded byte for byte |
| `fuzz_varint` | `wire::read_varint` |
| `fuzz_key_pattern` | `match::key_pattern::make` and the pattern set relations |
| `fuzz_frame_reassembler` | the wire-level frame reassembler decode path |
| `fuzz_reassembler` | `wire::decode_udp_fragment_header` + the bounded datagram `reassembler::feed` |
| `fuzz_ring_attach` | `shm::broadcast_ring::attach` over a raw control image or a created ring with rewritten bytes (the retired stamp among them), then a bounded run of ring, channel and adoption-repair steps with the control region rewritten between them |
| `fuzz_udp_envelope` | the UDP envelope, ARQ kind (peek, ack, segment), fragment header and handshake decoders, each accepted value round-tripped through its encoder, and `udp_reliable_arq` driven by input-chosen submits, acks and segments under its window invariant |
| `fuzz_stream_inbound` | `stream_inbound` and its no-progress timer, driven by input-chosen feeds, drains and virtual clock steps; the delivered frames, concatenated, must be a prefix of the bytes fed |
| `fuzz_session_sequence` | two real peer sessions over the inproc listen/dial rendezvous, driven by up to 64 input-chosen operations (drive, injected encoder-built, raw and unknown-type frames, subscribe, declare, heartbeat, teardown), with the lifecycle invariants checked after every operation |
| `fuzz_crc_serial` | `crc_serial_inbound` over encoded serial streams with drops, bit flips, insertions, duplicated anchors and truncation, fed in input-chosen chunks; every emitted frame is well formed, an unmutated stream round-trips exactly, and a mutation loses at most the frames it touched |

Each harness is fail-closed by construction: every field is range-checked before it indexes, and a fresh decoder,
reassembler, ring or session pair is built per input, so no state crosses inputs. A contract violation prints
`plexus fuzz oracle violated: <contract>` and aborts. `fuzz_ring_attach` caps what it derives from the input (a 64 KiB
control image, a 1 MiB slab, 64 ring steps) and allocates each region at exactly its size, so AddressSanitizer flags
any access past a mapped region. Run it with `-rss_limit_mb=2048`.

### The planted twin

`fuzz_crc_serial_planted` is built from `fuzz_crc_serial.cpp` with `PLEXUS_FUZZ_PLANT_VIOLATION`, which inverts the
round-trip leg of the oracle, so it fails on its first input. CI requires it to exit nonzero with the oracle's message:
a green run of the twin would mean an oracle violation does not reach the exit status, and every other green run would
prove nothing. It has no replay test and is not fuzzed.

## Seeds

`seeds/plexus_fuzz_seeds` writes a starting corpus for every harness into `<corpus-root>/<harness>/`, built from the
real encoders (valid frames, boundary values, and the regression inputs of past findings). The build-tree corpus is
`<build>/tests/fuzz/corpus/`; the `fuzz_seed_corpus` target fills it:

```bash
cmake --build build-fuzz --target fuzz_seed_corpus
```

The corpus is a build artifact and is never committed; `tests/fuzz/corpus/` is ignored. libFuzzer writes every
coverage-increasing input into the directory it is given, so runs only ever see the build-tree corpus.

## The replay gate

Every harness registers a `<harness>_replay` test that executes each stored input once without mutation (`-runs=0`)
and fails on a nonzero exit, an empty corpus, or a reported run count below the number of inputs. The tests require the
`fuzz_seed_corpus` fixture, so ctest generates the seeds before the first replay:

```bash
ctest --test-dir build-fuzz --output-on-failure --no-tests=error -R _replay
```

A replay is a regression check over stored inputs, never a stand-in for a fuzzing run.

## Building the fuzz tree

libFuzzer requires Clang. The harness targets compile only when the compiler is Clang (the CMake subtree skips with a
warning otherwise) and are gated behind `PLEXUS_BUILD_FUZZ_TESTS`. `fuzz_ring_attach` needs the shared-memory backend:

```bash
cmake -S . -B build-fuzz -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
    -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON -DPLEXUS_BUILD_FUZZ_TESTS=ON
cmake --build build-fuzz -j4
```

Each target is built with `-fsanitize=fuzzer,address,undefined -fno-sanitize-recover=all -O1 -g`. Keep this tree
separate from the ThreadSanitizer tree below: libFuzzer does not combine with ThreadSanitizer.

## A local campaign

libFuzzer writes crash artifacts into the working directory unless told otherwise, so run from a scratch directory or
pass `-artifact_prefix`. A bounded campaign over one harness, growing the build-tree corpus:

```bash
h=fuzz_crc_serial
mkdir -p fuzz-artifacts
build-fuzz/tests/fuzz/$h -max_total_time=60 -timeout=25 -rss_limit_mb=2048 -print_final_stats=1 \
    -artifact_prefix=fuzz-artifacts/$h- build-fuzz/tests/fuzz/corpus/$h
```

Any crash, sanitizer report or nonzero exit is a finding. Reproduce one by passing the artifact as the only input,
which runs it once:

```bash
build-fuzz/tests/fuzz/$h fuzz-artifacts/$h-crash-<sha1>
```

A finding is kept as a unit regression in the affected subsystem's tests and as a seed in `seeds/`, so the replay gate
covers it from then on. To shrink a grown corpus to a coverage-equivalent set:

```bash
mkdir -p minimized/$h
build-fuzz/tests/fuzz/$h -merge=1 -timeout=25 -rss_limit_mb=2048 minimized/$h build-fuzz/tests/fuzz/corpus/$h
```

## The CI workflow

`.github/workflows/fuzz.yml` runs on pushes to `master`, `develop` and `milestone/**`, on pull requests to `master` and
`develop`, weekly on a schedule, and on manual dispatch. It runs one job:

1. Build every harness with Clang 18 and generate the seeds.
2. Assert that every listed harness and the planted twin were built and that the build produced no harness the list
   omits, then that every harness's corpus holds at least one input.
3. Run the replay tests over the generated seeds.
4. Restore the accumulated corpus from the store into the build-tree corpus.
5. Fuzz every harness for 60 seconds with `-seed=1` on pushes and pull requests, or 300 seconds with a random seed on
   the schedule and on dispatch. The harnesses run in parallel, one per runner core, each at the full budget. A
   harness fails the run on a nonzero exit or when it reports zero executed inputs.
6. Require the planted twin to fail with the oracle's message.
7. Minimize every corpus with `-merge=1` and upload the result as the `minimized-corpus` artifact.
8. On `master` only, never from a pull request, push the minimized set back to the store.
9. On failure, upload the crash inputs and the per-harness logs (which carry the seed used) as `fuzz-findings`.

The store is the repository `skrede/plexus-fuzz-corpus`, one directory per harness. It is reached over SSH only,
for both restore and push, through the repository variable `PLEXUS_FUZZ_CORPUS_URL` (a `git@github.com:` URL) and the
secret `FUZZ_CORPUS_DEPLOY_KEY` (the private half of a deploy key with write access on the store). The key reaches
disk only by redirection and github.com's published ed25519 host key is pinned. When the variable or the key is not
set, as for pull requests from forks, the run fuzzes from the generated seeds only and says so in a notice. A store
that is configured but unreachable fails the run, and a minimization that leaves any harness with zero inputs refuses
to push.

## The sanitizer trees

The fuzz gate is the untrusted-input leg of a broader sanitizer matrix. The two companion trees:

### AddressSanitizer + UndefinedBehaviorSanitizer (all backends)

```bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=all -g" \
    -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_CRYPTO_BACKEND=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON \
    -DPLEXUS_AFFINITY_CHECKS=ON -DPLEXUS_BUILD_TESTS=ON
cmake --build build-asan -j4
ctest --test-dir build-asan -j4 --timeout 120
```

`-fno-sanitize-recover=all` makes any UB the first report fatal. Zero reports is the pass bar; the datagram reassembler
(the untrusted-input surface) is included.

### ThreadSanitizer (full transport matrix)

ThreadSanitizer must cover every transport, not just the shared-memory ring; the backend matrix mirrors the
AddressSanitizer tree, only the sanitizer flag differs. The genuinely threaded surface is the lock-free shared-memory
ring plus the cross-process notifier; the transport data paths are single-io-thread by design, so ThreadSanitizer over
them is a no-accidental-sharing assurance.

```bash
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer -g" \
    -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_CRYPTO_BACKEND=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON \
    -DPLEXUS_AFFINITY_CHECKS=ON -DPLEXUS_BUILD_TESTS=ON -DPLEXUS_WARNINGS_AS_ERRORS=OFF
cmake --build build-tsan -j4
ctest --test-dir build-tsan -j4 --timeout 120
```

Do not add `,address` or `,undefined` to the ThreadSanitizer flags; it does not combine with AddressSanitizer. A few
`-Wtsan` warnings from asio's fenced-block (`atomic_thread_fence` is uninstrumentable under ThreadSanitizer) are benign
and inherent to running asio under ThreadSanitizer; they are not data races.

### Cross-process notifier stress

The cross-process shared-memory notifier and teardown path is the highest-value race target. A targeted
high-iteration run under heavy contention discriminates a deadline slip (a timing artifact under load: completes, just
slow) from a lost-wakeup hang (the notifier word is signaled before the consumer parks). A run that completes within
the per-test timeout is hang-free; a run that hits the timeout without progress is a lost wakeup and a release blocker,
not a flake.

```bash
for i in $(seq 1 50); do
    ctest --test-dir build -j16 --timeout 60 -R 'shm\.(notifier|teardown)' --output-on-failure \
        || echo "ITER $i NONZERO"
done
```
