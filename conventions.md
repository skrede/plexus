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

Each scope does **one thing**, and the smaller that one thing is, the cleaner the code reads: a
library does one thing, a namespace groups the types and functions working toward one thing, a class
does one thing, a function does one thing. A unit that runs over its ceiling is usually the "one
thing" drawn too broad — decompose it, do not widen the budget. (The genuinely-cohesive whole that
splitting would *harm* is the registered exception below, not the default.)

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

Every exception is registered in **`EXCEPTIONS.md`** — the single, complete list of every
sanctioned over-limit unit, each with its justification. There is **no in-code marker**: the
justification is bookkeeping, and bookkeeping never belongs in the code. The size gate sanctions
exactly the files `EXCEPTIONS.md` lists and fails on any unlisted overage, or on a stale row whose
file has since dropped under the ceiling. There are no silent exceptions.

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
- sign-posting or narration ("this section does X", "first we … then we …");
- bookkeeping or process markers — size-gate justifications, ledgers, "see the doc" pointers,
  anything whose audience is the maintainer's process rather than the code's reader. That lives in
  `EXCEPTIONS.md` or the planning tools, never in the source.

A comment that merely repeats the next line is noise: it steals screen space, competes with
the code, and rots out of sync. Cut it. Keep a multi-line comment tight — no blank `//`
separator lines padding it out.

A comment longer than a short tag goes on its **own line above** the code it explains, never
trailing. A long trailing comment crowds the statement and forces the formatter to wrap the code
beneath it into an ugly multi-line shape; the line above is where the eye looks first anyway. A
brief trailing tag on a data line — a unit, an enumerator's value — is fine where it reads as part
of that line.

Comments count toward the size budget. A file that is 30% comments is usually
30% over-commented, not a candidate for a bigger budget — trim first. Do not write a novel.

## Includes

Internal project includes (`#include "..."`) come first, third-party libraries second,
standard-library headers third. Order matters only when something must precede something else
for a real reason.

- The three major sections are separated by one blank line.
- Within a major section, group includes by folder. The folder groups are ordered
  **alphabetically by folder path** and separated by one blank line. Only one blank line between
  any two groups.
- Within a folder group, sort by the include's **file-name length, shortest first**; file names
  of equal length sort alphabetically.

## Naming

- A member holding a callback or hook (a `move_only_function`) takes a `_cb` suffix and groups with
  the other callbacks in the member list — the suffix marks it as an injected behavior, not state.

## Construction

- Initialize a class's members in the **constructor's init list, never in-declaration** — one place,
  one source of truth, so two defaults can never drift apart. A class with a constructor carries no
  in-declaration member initializers.
- A member wanting a default value is exactly what a constructor is for: **give the type a constructor**
  and put the default in its init list. Do not leave the default in-declaration. The only type without a
  constructor is a pure aggregate that carries *no* defaults at all — brace-initialized at every use, no
  in-declaration initializers; it has nothing to move. The moment one member wants a default, the type
  takes a constructor.
- **The constructor's init list is written in member-declaration order.** Members initialize in
  declaration order regardless of the init-list order, so the two must agree (no `-Wreorder`). This
  has a consequence for the member triangle below: a member consumed by another member's constructor
  must be **declared before** its consumer, and that initialization dependency **overrides the length
  triangle** — the triangle is the shape where dependencies leave it free, never at the cost of a
  correct construction order.

## Class layout

Order a class body so the data is visible before the operations on it:

```
class T
{
    // leading private nested helper types (if any)

public:
    constructor(s)
    public functions

private:
    members          // data first, right after private:
    private functions
};
```

A private nested helper type a class needs may lead the class body, before `public:`. The member
triangle (above) orders the members within the leading private data block, subject to the
initialization-order constraint in **Construction**.

## Types and attributes

- **Spell out return types.** Do not write `auto` as a function's return type — the named type is
  what a reader looks for first, and `auto` makes the code harder to read and understand. The only
  exception is a return type that is genuinely unspellable (a lambda / closure), where `auto` is
  unavoidable. `auto` for a *local variable* is fine where the initializer makes the type obvious.
- **No `[[nodiscard]]`. Ever.** Do not decorate declarations with attributes the compiler does not
  require — they are visual noise. Keep attributes to the few the language genuinely needs.

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
- **Raw pointers are an anti-pattern — avoid them as far as possible.** Use a `unique_ptr` for sole
  ownership and a `shared_ptr` for genuinely shared ownership; for everything else, pass and store
  **references**. The system composes this way: an owner holds the long-lived utilities and hands
  them out by reference (a context object the parts reach through), so a non-owner never needs a
  pointer. A raw pointer survives only where absence is a real, modelled state (a nullable lookup
  result) — and even there prefer a reference or `std::optional` where it fits.
- A non-owning relationship that *must outlive* its holder is a **reference, not a raw pointer** — a
  reference says "not optional, not owned" without the nullability a pointer implies. Store it as
  `std::reference_wrapper<T>` when it lives in a container that needs assignability (a `std::vector`
  element, say). Reach for a smart pointer only when there is real shared or transferred ownership.
- No `std::shared_from_this` / `std::enable_shared_from_this`.
- No per-callback liveness guards that paper over a posted task outliving its target. If a
  posted closure can run after its target dies, fix the ownership and teardown sequencing —
  do not add a flag to guard every access.

## Formatting and tooling

- `.clang-format` is the formatter, and it is authoritative on its own for the mechanical rules:
  185-column lines, comments never reflowed to fit the column, short functions never auto-collapsed
  onto one line (the author decides), and includes left unsorted so the hand-ordering below holds.
  Run it.
- `.clang-tidy` is the linter and the size/complexity gate. It fails the build.
- Apply both across the whole tree. A change is not done until both are clean — or its overage is a
  registered exception.

### Shape (readability guidelines, not rigid rules)

These shape the code for the human eye. They are judgment calls — a case that reads better the other
way is a fine exception. The intent is the rule; the shapes are how it usually looks. The point is to
make the next name easy to find.

- **Downward waterfall.** When a signature or declaration wraps, each continuation line is *longer
  than or equal to* the one above it, so the wrapped arguments slope downward and the eye falls to
  the next one.
- **Member triangle.** Order a class's members by overall declaration-line length, shortest first —
  the type name dictates the order — so the members form a downward slope. A **single space** precedes
  each name; names are *not* aligned into a column (only consecutive `=` assignments are aligned).
  Semantic grouping wins when it helps: keep the callbacks together even if a shorter line lands among
  them. The triangle yields to initialization order (see **Construction**); shape serves reading,
  never the reverse.
- **Logical blank lines.** A blank line inside a function separates logical parts — set-up from the
  work, one phase from the next — the way a paragraph break does. Separate each member function from
  the next with a single blank line; the leading data members are packed together with none.

## English

American spelling: "stabilizing", not "stabilising"; "color", not "colour".

## Commit messages

Format:

```
{Prefix}: {summary sentence}.

- {what was done, one line per item}
- {another item if applicable}
```

The summary line is brief and descriptive; the bullet list expands on it. A single-item commit
may omit the bullets. Allowed prefixes: **Feature, Fix, Refactor, Docs, Examples, Optimization,
Test, Build, WIP**. Use `WIP` when the commit does not compile.

The no-planning-artifacts rule applies to commit messages too: never reference a phase number,
plan or task ID, requirement or invariant label, or any other planning-tool artifact in a commit
message.
