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
| lib/plexus-api/include/plexus/node.h | file | One cohesive public facade; the typed member factories and the private endpoint seams share the engine + subscription/peer/served-fqn state, so splitting the surface scatters that shared state (the shm-wiring glue is extracted to detail/node_shm_wiring.h). |
| lib/plexus-core/include/plexus/io/routing_engine.h | file | One cohesive node-level engine; the demand-driven public verbs share the borrowed substrate members and the posted observer fan-out, so splitting the API scatters that shared state (the sink-install, sink-maker, and posted-dispatch clusters are extracted to detail/). |
| lib/plexus-core/include/plexus/io/message_forwarder.h | file | One cohesive pub/sub engine; splitting scatters the shared registry + scratch state (the attach gate and the publish/object fan steps are extracted to detail/forwarder_fanout.h). |
| lib/plexus-core/include/plexus/io/procedure_forwarder.h | file | One cohesive req/res correlation engine; splitting scatters the per-peer outstanding table + the reused scratch/active-request state (the rpc fan + send-frame helpers are extracted to detail/procedure_fanout.h). |
