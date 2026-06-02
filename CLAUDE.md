Do not add Co-Authored-By lines to commit messages.

Do not be sycophantic nor agreeable to be appealing. The user values rigor, honesty, and
objectivity — not sycophancy.

Use American English: e.g., "stabilizing" not "stabilising", and "color" not "colour".

Never refer to planning-tool artifacts (phase/milestone/plan/task numbers, GSD/.planning IDs
or keys) anywhere in the codebase — including commit messages, code, comments, documentation,
and examples.

Never do git tagging, never merge, never push, no force flags, and do not override .gitignore.

Commit message format:
{Prefix}: {summary sentence}.

- {what was done, one line per item}

Allowed prefixes: Feature, Fix, Refactor, Docs, Examples, Optimization, WIP. Use WIP if the
code does not compile. Aim for one commit per GSD plan.

Branching model: master (releases) -> develop (integration) -> milestone/<version> (work).
Commit to the milestone branch; never push. Merges are the user's.

.planning/ is NOT committed to the code repository; it has a separate, independent shadow git
repo (`git -C .planning ...`). Do not override .gitignore to add it.

## What plexus is

plexus is a standalone, header-only-core, policy-based C++20 middleware (pub/sub + req/res),
extracted from and porting the proven cores of vagus-plexus. It has **zero dependency on vagus
or vagus-core**. `mdnspp` is a dependency (discovery). plexus is **serializer-agnostic**: it
moves opaque bytes; no serializer ever lives in plexus.

Architecture invariants:
- Header-only generic core; heavy/backend code (asio, OpenSSL/mbedTLS) lives in separate,
  compiled, CMake-gated targets. Non-applicable features are disabled targets, not #ifdef soup.
- One compile-time `Policy` bundles the hot-path substrate (executor + socket + timer +
  memory traits + byte-owner). Cold-path services (discovery, logger) are runtime-injected
  virtual interfaces in compiled adapters — not template policies.
- No allocation on the steady-state hot path (allocate at setup; deterministic message loop).
- `wire_bytes` = a non-owning view + a policy-selected owner handle whose lifetime bounds it.
- Design so as not to preclude an MCU profile (RTOS / lwIP / serial; bounded, exception-aware
  core). MCU is a later target, not a day-one deliverable — design-in the seams, defer the port.

## C++ conventions

Generate idiomatic, cross-platform C++20 (macOS, Linux, Windows). Follow asio/modern-C++
library conventions; existing conventions in the project take precedence — ask if unsure. No Boost.

Header guards (not pragma once): HPP_GUARD_<NAMESPACE>_<FOLDER>_FILENAME_H. Do not add a
matching `// namespace X` after a closing namespace brace, nor a matching comment after the
`#endif` of an include guard.

Include order: internal project includes (`"..."`) first, third-party (`<...>`) second, standard
library third. One blank line between these major sections. Within a section, group by folder
(blank line between groups); within a group, sort by length, then alphabetically.

Function/file size: aim for 5-15 LOC functions (25 max) and ~100 LOC files (200 max).
Readability over dogmatic SOLID/DRY; never split a unified concept just to hit a number.
