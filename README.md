# plexus

[![CI](https://github.com/skrede/plexus/actions/workflows/ci.yml/badge.svg)](https://github.com/skrede/plexus/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/skrede/plexus/branch/master/graph/badge.svg)](https://codecov.io/gh/skrede/plexus/branch/master)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](LICENSE)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](#)
[![platforms: Linux | macOS | Windows](https://img.shields.io/badge/platforms-Linux%20%7C%20macOS%20%7C%20Windows-lightgrey.svg)](#)
[![status: preview](https://img.shields.io/badge/status-preview-orange.svg)](#status)

A standalone, header-only-core, policy-based C++20 middleware — pub/sub and request/response
over a swappable executor + networking backend.

plexus is serializer-agnostic: it moves opaque bytes. One compile-time `Policy` selects the
hot-path substrate (executor + networking); cold-path services sit behind runtime interfaces.
The core is header-only and platform-agnostic, with additive backends — asio, POSIX shared
memory, in-process, DTLS, and a FreeRTOS edge target — chosen at configure time.

## Status

**Preview — pre-1.0.** The public API is still changing between releases and plexus is not yet
recommended for production use. The repository is public primarily to run cross-platform CI in
the open; external contributions are not being solicited yet — see
[CONTRIBUTING.md](CONTRIBUTING.md) for how the project is built and organized.

## Building

Requires a C++20 compiler (GCC 14, Clang 18, MSVC, or clang-cl) and CMake ≥ 3.25.

```bash
cmake -S . -B build -G Ninja \
  -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON \
  -DPLEXUS_BUILD_TESTS=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

Backends and tools are additive and default to `OFF`; enable only what you need. The full set of
`PLEXUS_*` options is defined in the top-level `CMakeLists.txt`.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
