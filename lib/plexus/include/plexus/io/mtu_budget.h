#ifndef HPP_GUARD_PLEXUS_IO_MTU_BUDGET_H
#define HPP_GUARD_PLEXUS_IO_MTU_BUDGET_H

#include <cstddef>

namespace plexus::io {

// A static per-channel PER-DATAGRAM payload budget that the fragment / oversize
// path consults. It is NOT the largest message a channel can carry — once the
// fragment/reassemble block composes in, a payload larger than this budget is
// SPLIT across numbered datagrams rather than rejected, so this value caps a
// single datagram's bytes, not a logical message. The hard message ceiling and
// the reassembly-memory bound live with the reassembler, not here.
//
// The budget is deterministic — there is no path-MTU discovery (which would
// couple the data plane to ICMP and a moving network state), so the value is
// fixed at setup and friendly to an MCU / lwIP profile.
//
// Ceiling arithmetic (the direction is UP — fragmentation carries the rest):
//   * conventional Ethernet path: a 1500-byte L2 MTU minus the IPv6 (40) + UDP
//     (8) headers leaves 1452 bytes of UDP payload. The default 1400 sits just
//     below that with headroom for the 9-byte fragment sub-header, so a single
//     fragment never relies on IP fragmentation on a standard path.
//   * jumbo-frame path: a 9000-byte L2 MTU minus the same 48 header bytes leaves
//     8952; field_payload_ceiling (8192) is a clean 8-KiB-class value below it,
//     leaving room for the fragment sub-header and the IP/UDP headers inside one
//     jumbo frame. A caller on a jumbo-capable path opts in by passing it.
//
// The default stays the conservative single-standard-Ethernet-datagram floor
// (behavior-preserving — every existing path keeps its byte-for-byte framing);
// the raised field ceiling is the documented intent exposed as the opt-in knob.
// Empirical tuning of the exact per-fragment value against a loss/throughput
// sweep is a later, evidence-driven step, not a feel-picked constant here.
struct mtu_budget
{
    static constexpr std::size_t field_payload_ceiling = 8192;

    std::size_t max_payload = 1400;
};

}

#endif
