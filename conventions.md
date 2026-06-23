# plexus coding conventions

How we write code in plexus. The goal is a codebase that is **lean and mean** — small,
readable, fast — with no loss of features, capability, or speed. These conventions are
written for people; they apply to everyone, including the agents that write much of the
code. This file is authoritative for code style. When something here conflicts with habit,
this wins.

## No planning artifacts

Planning lives in the planning tools, never in the code. Never reference an ID, key, or label
produced by a planning or issue-tracking tool — phase numbers, milestone names, plan or task
numbers, requirement IDs, invariant labels (`INV-2`, `SER-01`, …), or any `.planning`/GSD
artifact — anywhere in the codebase: identifiers, comments, documentation, examples, and commit
messages.

State the *thing itself*, not its planning label. A comment explaining a real invariant says it
in plain words ("the core holds no platform-specific code"); it never cites "INV-2". If a reader
would have to open a planning document to decode a comment, the comment is wrong. The same goes
for narrative scaffolding from how the code was built — "the slice", "the gate", "this milestone"
— which means nothing to a reader of the finished code. Cut it.

## Language and portability

- Idiomatic C++20. Use the language; do not reinvent what the standard library, asio, or
  boost already do well.
- The code must compile and run on macOS, Linux, and Windows. Keep platform-specific code
  inside clearly isolated backends, never in the core.
- Follow the conventions of modern C++ libraries (standard library, asio, boost). Existing
  project conventions take precedence. If you are unsure, or a popular idiom contradicts the
  project, ask rather than guess.

## File and function size

The limits, lines **including comments and blank lines** — the budget is the whole thing as
it sits on screen:

| Unit     | Target     | Hard ceiling |
| -------- | ---------- | ------------ |
| Function | 5–15 lines | 25 lines     |
| File     | ~100 lines | 200 lines    |

Readability is the goal, not SOLID or DRY orthodoxy. Group code that is read together. Split
where it genuinely separates responsibilities — never to chase a number.

Forbidden:

- **Salami-slicing** — carving a cohesive unit into fragments just to fit a limit.
- **One-function files** — a file that exists only to hold a single function.
- **Artificial purity** — a wrapper or layer that adds indirection without separating a real
  responsibility.

Keep public headers lean by moving internal helpers into a `detail/` directory and a
`detail::` namespace. Make `detail/` files and `detail::` types as needed — but a `detail/`
file is still a real file and obeys these same rules.

Enforcement: `.clang-tidy` runs size and cognitive-complexity checks and **fails the build**
over the ceiling. A change is not done until it is within budget or its overage is a
registered exception.

### Exceptions

A file or function may exceed its ceiling only when it is a single cohesive whole that
splitting would *harm* — scattering shared state across files, or forcing an artificial-purity
layer. An exception is a considered decision, not a fallback; prefer decomposition, and reach
for an exception only when decomposition would make the code worse.

Every exception is registered in two places that must agree:

1. A one-line marker at the site:
   `// over-limit: one cohesive pub/sub engine; splitting scatters the shared registry + scratch state`
2. An entry in **`EXCEPTIONS.md`** — the single, complete list of every sanctioned over-limit
   unit, each with its justification.

The size gate allows exactly the registered units and fails on any new or unlisted overage.
CI cross-checks that `EXCEPTIONS.md` and the in-code markers match. There are no silent
exceptions.

## Comments

The codebase is written for people to read. A comment must say something the code itself
cannot. **The default is no comment** — clear names and signatures carry the meaning.

Write a comment only when it does one of:

- **(a)** cites a source — a paper, RFC, spec, URL, bug report, or standards clause;
- **(b)** names a non-obvious algorithm or technique so a reader can look it up;
- **(c)** explains a non-obvious design choice in one or two sentences — a bug workaround, a
  deliberate tradeoff, or a contract / ordering / side-effect / invariant that is real but not
  visible from the code.

Delete everything else. Never write:

- comments that restate a name, signature, type, or obvious control flow in more words;
- tables or lists that transcribe the lines of code beneath them;
- sign-posting or narration ("this section does X", "first we … then we …").

A comment that merely repeats the next line is noise: it steals screen space, competes with
the code, and rots out of sync. Cut it.

Comments count toward the size budget. A file that is 30% comments is usually
30% over-commented, not a candidate for a bigger budget — trim first. Do not write a novel.

## Includes

Internal project includes (`#include "..."`) come first, third-party libraries second,
standard-library headers third. Order matters only when something must precede something else
for a real reason.

- The three major sections are separated by one blank line.
- Within a major section, group includes by folder into intermediate sections, also separated
  by one blank line. Only one blank line between any two sections.
- Within a section, sort first by line length, then alphabetically among equal lengths.

## Header guards and namespaces

- Use header guards, not `#pragma once`. Format: `HPP_GUARD_<NAMESPACE>_<FOLDER>_FILENAME_H`.
- Do not write a `// namespace X` comment after a closing namespace brace.
- Do not write a comment after `#endif` restating the guard macro.

## API design

- No raw pointers in public APIs. Pass by `const&` (or by value, `std::span`, or a smart
  pointer where that is the right ownership story).
- Distinguish three things and never conflate them: **required** (no default),
  **required-with-default** (the override is optional), and **`std::optional`** (absence is
  itself meaningful). Do not use `std::optional` as a stand-in for a default.
- Pre-release, there is no `[[deprecated]]`. Delete a superseded type outright — there are no
  external users to cushion.

## Lifetimes and ownership

- Structural, single-owner lifetimes. The owner sequences teardown.
- No `std::shared_from_this` / `std::enable_shared_from_this`.
- No per-callback liveness guards that paper over a posted task outliving its target. If a
  posted closure can run after its target dies, fix the ownership and teardown sequencing —
  do not add a flag to guard every access.

## Formatting and tooling

- `.clang-format` is the formatter. Run it.
- `.clang-tidy` is the linter and the size/complexity gate. It fails the build.
- Apply both across the whole tree. A change is not done until both are clean — or its overage
  is a registered exception.

## English

American spelling: "stabilizing", not "stabilising"; "color", not "colour".
