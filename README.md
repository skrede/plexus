# plexus

[![CI](https://github.com/skrede/plexus/actions/workflows/cross-compiler.yml/badge.svg)](https://github.com/skrede/plexus/actions/workflows/cross-compiler.yml)

A standalone, header-only-core, policy-based C++20 middleware — pub/sub and request/response
over a swappable executor + networking backend, with `mdnspp` for discovery.

plexus is serializer-agnostic (it moves opaque bytes) and carries **zero dependency on vagus**.
It is being built by porting the proven cores of `vagus-plexus` into a clean, backend-agnostic
shape, mirroring the abstraction philosophy of `mdnspp` (one compile-time `Policy` for the
hot-path substrate; cold-path services behind runtime interfaces).

Status: greenfield. The first milestone is a single production-quality primitive (one primitive,
asio + inproc backends) that validates the policy seam before the full port.

> This repository is independent. The accumulated design record and prior art live in the
> vagus planning archive — see `.planning/reference/VAGUS-PLANNING-SOURCE.md`.
