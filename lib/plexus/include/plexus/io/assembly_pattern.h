#ifndef HPP_GUARD_PLEXUS_IO_ASSEMBLY_PATTERN_H
#define HPP_GUARD_PLEXUS_IO_ASSEMBLY_PATTERN_H

namespace plexus::io {

// The datagram assembly pattern: a documentation-only contract (NOT a uniform
// interface and NOT a coordinator object) capturing HOW a concrete datagram
// channel and transport assemble the core building-blocks around an irreducible
// backend pump. A concrete backend owns each block as a direct member and drives
// it directly — zero indirection, no virtual dispatch. This header carries no
// declarations; it pins the seam so every datagram backend (the plain UDP pump
// and any future crypto pump alike) wires the SAME blocks in the SAME order.
//
//   A concrete datagram channel OWNS, in this destruction-safe order:
//     1. the backend pump handle (a borrowed ref: the shared socket / a TLS pump)
//     2. the reliability algorithm(s) it composes (the selective-repeat ARQ)
//     3. the send_queue block (its drain calls a send-sink that hits the pump)
//     4. [optional] a handshake_gate block (buffers outbound until a ready edge)
//     5. channel-private wire scratch buffers (the envelope/segment encode space)
//   and DRIVES them directly:
//     - send():            [gate.submit -> on-ready ->] send_queue.enqueue -> pump
//     - deliver_inbound(): pump -> wire codec -> ARQ/dedup -> post_on_data
//     - dtor:              set m_open=false; cancel the retransmit timers FIRST;
//                          the owned blocks are then destroyed in reverse order.
//
//   A concrete datagram transport OWNS:
//     - the pump (the shared bound socket / the crypto pump)
//     - the inbound demux (the generic sender -> channel map + flood cap)
//     - the dial-resolution retry ladder
//     - the pending_dial_registry (the half-open dial table + the accepted table,
//       with copy-before-erase resolve and deferred-destroy fail)
//     - the mtu_budget (the static per-channel payload budget it threads into each
//       minted channel)
//
// A future crypto datagram backend assembles the SAME blocks around its own pump:
// the same send_queue (its send-sink writes the encrypted record to the socket),
// the same handshake_gate as the open-before-data edge (ready = the secure session
// is established and the peer verified), and the same pending_dial_registry. Only
// the irreducible pump mechanism differs; the assembled blocks do not fork.

}

#endif
