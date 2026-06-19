# Over-limit exceptions

The single, complete list of every unit that is sanctioned to exceed the size ceiling
in `conventions.md` (functions <=25 lines, files <=200 lines, comments included). An
exception is earned only when a unit is one cohesive whole that splitting would *harm* —
scattering shared state across files or forcing an artificial-purity layer.

Every row here must agree with an in-code `// over-limit: <reason>` marker at the site,
and the justification must match. The size gate cross-checks both directions and fails
the build on any unlisted overage, any marker without a row, or any row whose unit is no
longer over the limit. There are no silent exceptions.

| Path | Kind | Justification |
| ---- | ---- | ------------- |
