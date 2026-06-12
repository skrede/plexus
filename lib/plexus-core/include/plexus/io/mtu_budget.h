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
// The default is the RFC 9000 §14 conservative datagram bound: a tunnel/VPN path MTU
// below the fragment size blackholes silently (no ICMP, no PMTUD here), so 1200 is the
// safe floor an arbitrary IP path always carries. It is tunable UP (never silently) —
// field_payload_ceiling (8192) is the opt-in jumbo-class knob a caller on a known wide
// path passes; fragmentation carries any logical message larger than the chosen value.
struct mtu_budget
{
    static constexpr std::size_t field_payload_ceiling = 8192;

    std::size_t max_payload = 1200;
};

}

#endif
