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
| lib/plexus-core/include/plexus/io/handshake_fsm.h | file | One transition table over a single handshake transcript; the on_* events and resolve helpers all advance the shared m_state/m_peer_id/m_inbound_pending/m_complete_emitted transcript, so splitting them scatters that shared state (the wire/config types are extracted to handshake_protocol.h). |
| lib/plexus-core/include/plexus/io/peer_session.h | file | One cohesive per-peer bridge; the public seam setters and the receive/staleness/teardown lifecycle share the borrowed ctx/negotiator/fsm + the per-incarnation counters and seam members, so splitting the surface scatters that shared state (the FSM-drive, handshake-encode, consumer-register, and deliver helpers are extracted to detail/peer_session_*.h). |
| lib/plexus-core/include/plexus/io/peer_session_registry.h | file | One cohesive multi-peer connection map; the public query/lifecycle verbs all reach the shared m_slots ownership table + the borrowed m_build context, so splitting the surface scatters that shared state (the session-build and seam-wiring helpers are extracted to detail/peer_session_build.h). |
| lib/plexus-core/include/plexus/io/shm/broadcast_ring.h | file | One Vyukov-sequenced lock-free ring; the claim/commit/consume cursor protocol + the seq_cst overwrite-vs-pin Dekker handshake are one indivisible whole over the shared header/cells/cursor atomics — the in-class index accessors only bind member state to the layout (in ring_layout.h) and pulling them out threads the ring members through every hot path, scattering the protocol. |
| lib/plexus-core/include/plexus/io/shm/shm_mux_member.h | file | One cohesive mux_member contract; the dial/listen/can_acquire/companion verbs all drive the OWNED registry (the sole ring-lifecycle owner) + the per-fqn geometry map, so splitting the surface scatters that shared ownership state (the channel, the consumer, the preference hook, and the acquire/resolve glue are extracted to sibling + detail/ headers). |
| lib/plexus-core/include/plexus/io/shm/shm_topic_registry.h | file | One cohesive ring-lifecycle owner; the acquire/release/sink/drain verbs and the nested entry all share the m_entries ownership table + the per-entry notifier-arm/teardown ordering, so splitting the surface scatters that shared lifecycle state (the result types are extracted to shm_acquire_result.h, the open/mint/join glue to detail/shm_topic_open.h). |
| lib/plexus-core/include/plexus/io/shm/ring_layout.h | file | Cited cross-process ring layout + named lock-free algorithms (std/parking_lot 3-state word, Vyukov, Dekker mutual-announce) + the cross-process layout invariants; the why is load-bearing (D-08 keep-list) and trimming the citations would lose the ABI/ordering record. |
