# Continuous integration

The cross-compiler workflow (`.github/workflows/cross-compiler.yml`) builds plexus and
runs the full test suite across six compilers spanning Linux, macOS, and Windows. Three of
them — GCC on Linux, AppleClang on macOS, and MSVC on Windows — are also exercised by hand
during local bringup, so in CI they ride as regression guards that keep the cross-platform
signal alive after the one-off local runs end. The other three are exercised only in CI:
clang-cl on Windows, an alternate Homebrew Clang on macOS, and mainline Clang on Linux.
Together they cover the compilers that no local run touches.

## CI is additive signal, not the gate

The green CI matrix is **additive** cross-compiler coverage; it is **not the milestone
gate**, and it is not the definition of done. The definition of done stays
local per-host green on Windows and macOS plus the standing Linux green: an agent on each host
builds and runs the full suite, and those local runs are the arbiter of correctness. The
CI matrix closes out the cross-compiler coverage on top of that arbiter — it never replaces
it. Put plainly: CI is additive, not the gate.

## Operating notes

- The suite runs single-stream in CI (no parallel `-j`). This is deliberate: it keeps the
  run deterministic, because one known load-sensitive test flakes under parallel load and
  would otherwise poison an otherwise-green leg.
- Dependencies (asio, Catch2) resolve through the in-tree FetchContent fallback — nothing
  is preinstalled for them, so CI exercises the same fetch path a fresh checkout takes.
- Every third-party Action is pinned to a commit SHA.
