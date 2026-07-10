# Foxglove plot spot-check (manual)

The automated MCAP acceptance (`tests/integration/test_mcap_transcode.cpp`) proves a capture
**decodes**: it reads the container back through the mcap reader Summary (`NoFallbackScan`),
asserts an indexed Summary with a chunk index and statistics, checks that every `json` message
channel references only a `jsonschema` schema (the Foxglove doctor rule), and confirms the pose
channel joins the well-known `foxglove.Pose` schema from the codec-carried hint.

Headless execution **cannot** confirm that the capture actually **plots** in the Foxglove GUI —
"decodes" is a necessary floor, not the plot proof. A viewer can open and index a file yet render
nothing. This document is the manual protocol an operator runs to establish the plot claim, and
the recorded verdict below is the plot proof of record. Until it is run, the plot claim is
**UNPROVEN**.

## Produce a sample capture

```
cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_BUILD_MCAP_TRANSCODE=ON
cmake --build build -j4 --target mcap_basic_foxglove
./build/examples/recording/mcap_basic_foxglove
```

This writes `mcap_basic_foxglove.plxr` (the flat capture, drained through the bundled host
`file_sink`) and `mcap_basic_foxglove.mcap`. The publisher emits `foxglove.Pose`-shaped JSON on
`robot.pose`; the codec states its type id once and carries the neutral `pose` concept as its
`schema_hint`, and the host transcode joins that hint to the Foxglove well-known `foxglove.Pose`
JSON Schema. Nothing restates the type id and no schema row is hand-filled.

## Automated CLI floor (optional, run first)

If the `mcap` CLI is installed, confirm the container and schema decode before opening the GUI:

```
mcap info mcap_basic_foxglove.mcap
mcap cat mcap_basic_foxglove.mcap --json | head
```

Expect: a chunked, indexed file; a `robot.pose` channel whose schema is `foxglove.Pose`
(encoding `jsonschema`); and per-message JSON with `position` / `orientation` objects. This is
the same decode floor the automated test asserts — it is not the plot proof.

## Plot in Foxglove Studio

1. Open https://studio.foxglove.dev (or the desktop app) and load `mcap_basic_foxglove.mcap`.
2. Add a **3D** panel. The `robot.pose` topic should appear as a `foxglove.Pose`; enable it and
   confirm a pose (a position + orientation frame) renders in the 3D scene.
3. Add a **Plot** panel and plot `robot.pose.position.x` (and `.y`) over time. Confirm the series
   draws the ramp the example publishes (x = 0..7, y = 0..14).
4. "Opens" is not sufficient. The capture PLOTS only if a pose renders in the 3D panel AND the
   position series draws in the Plot panel.

## Encoding-support validation

The well-known schema is supplied as a **`jsonschema`** encoding. Confirm this encoding is
sufficient for the intended visualization:

- If the pose renders in the 3D panel and the position series plots, `jsonschema` suffices —
  record it in the verdict.
- If the 3D panel refuses the `jsonschema`-encoded pose (some richer Foxglove 3D types are
  exercised more reliably via protobuf), record that a protobuf-encoded schema is needed. The
  `mcap_schema` descriptor is encoding-agnostic, so this is a different provider/translator value
  (message encoding `protobuf`, schema encoding `protobuf`) — not a transcode change.

## Recorded verdict

- **Status:** UNRUN — plot UNPROVEN until an operator performs the steps above.
- **Result (PASS / FAIL):** _pending_
- **Date:** _pending_
- **Operator:** _pending_
- **Foxglove version:** _pending_
- **Encoding finding (jsonschema plots / protobuf needed):** _pending_
- **Notes:** _pending_
