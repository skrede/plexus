#ifndef HPP_GUARD_PLEXUS_DATAGRAM_ASSEMBLY_PATTERN_H
#define HPP_GUARD_PLEXUS_DATAGRAM_ASSEMBLY_PATTERN_H

namespace plexus::datagram {

// The datagram assembly pattern: a documentation-only contract (no declarations) pinning the order
// in which a concrete datagram channel and transport assemble the building-blocks around a backend
// pump, so every backend wires the same blocks in the same order.
//
//   A concrete datagram channel owns, in this destruction-safe order:
//     1. the backend pump handle (a borrowed ref: the shared socket / a TLS pump)
//     2. the reliability algorithm(s) it composes (the selective-repeat ARQ)
//     3. the send_queue block (its drain calls a send-sink that hits the pump)
//     4. [optional] a handshake_gate block (buffers outbound until a ready edge)
//     5. channel-private wire scratch buffers (the envelope/segment encode space)
//   and drives them directly:
//     - send():            [gate.submit -> on-ready ->] send_queue.enqueue -> pump
//     - deliver_inbound(): pump -> wire codec -> ARQ/dedup -> post_on_data
//     - dtor:              set m_open=false; cancel the retransmit timers FIRST;
//                          the owned blocks are then destroyed in reverse order.
//
//   A concrete datagram transport owns:
//     - the pump (the shared bound socket)
//     - the inbound demux (the sender -> channel map + flood cap)
//     - the dial-resolution retry ladder
//     - the pending_dial_registry (the half-open + accepted tables)
//     - the mtu_budget threaded into each minted channel

}

#endif
