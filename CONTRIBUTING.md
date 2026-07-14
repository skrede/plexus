# Contributing

plexus is an **early-stage, pre-1.0** project. The repository is public primarily so its
cross-platform CI runs in the open. This document explains how the project is built, tested, and
organized — mostly for anyone reading or forking the source.

## Contribution status

plexus is **not soliciting external contributions yet.** The public API is still changing
between releases and the internal structure is under active development, so external pull
requests are not being accepted at this stage.

Bug reports are welcome — please open an issue with a minimal reproducer, your compiler and its
version, your CMake version, and the platform. This policy will relax as the API stabilizes
toward 1.0; the README status line tracks where things stand.

## Building and testing

plexus requires a C++20 compiler (GCC 14, Clang 18, MSVC, or clang-cl) and CMake ≥ 3.25.
Backends and tools are additive and default to `OFF`.

```bash
cmake -S . -B build -G Ninja \
  -DPLEXUS_ENABLE_ASIO_BACKEND=ON -DPLEXUS_ENABLE_SHM_BACKEND=ON \
  -DPLEXUS_ENABLE_CRYPTO_BACKEND=ON \
  -DPLEXUS_BUILD_TESTS=ON -DPLEXUS_BUILD_EXAMPLES=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

The full set of `PLEXUS_*` options is defined in the top-level `CMakeLists.txt`. Warnings are
promoted to errors by default (`PLEXUS_WARNINGS_AS_ERRORS=ON`), and CI enforces this on every
platform.

## Coding conventions

The authoritative style specification is [CONVENTIONS.md](CONVENTIONS.md); the mechanical rules
live in `.clang-format`. In short: idiomatic, cross-platform C++20; header guards (no
`#pragma once`); small single-purpose functions and files; spelled-out return types; and
ownership expressed through smart pointers and references rather than raw pointers. Read
CONVENTIONS.md before changing code.

## Commit message format

```
{Prefix}: {summary sentence}.

- {what was done, one line per item}
- {another item if applicable}
```

Single-item commits may omit the bullet list. Allowed prefixes:

| Prefix | When to use |
|--------|-------------|
| `Feature:` | New user-visible capability or API surface. |
| `Fix:` | Bug fix or correctness-affecting change. |
| `Refactor:` | Internal reorganization with no user-visible behavior change. |
| `Build:` | Build system, CMake, packaging, or CI changes. |
| `Docs:` | Documentation-only changes. |
| `Examples:` | Changes to `examples/` only. |
| `Optimization:` | Performance change with no API or correctness impact. |
| `Test:` | Test-only additions or changes. |
| `WIP:` | Work in progress whose code does not yet compile. |

The summary line is brief and descriptive; the bullet list expands on the what.

## Branching model

```
master        <-  develop         <-  milestone/<version>
(releases)        (integration)       (active work)
```

Merge direction is always milestone → develop → master; reverse merges do not happen. `master`
holds released versions, `develop` is the integration branch, and `milestone/<version>` branches
host active work on a specific version.

## License

plexus is released under the [Apache License 2.0](LICENSE). Any accepted contribution is
licensed under the same terms; the Apache 2.0 grant in the standard "Submission of
Contributions" clause (Section 5) applies, with no separate CLA.
