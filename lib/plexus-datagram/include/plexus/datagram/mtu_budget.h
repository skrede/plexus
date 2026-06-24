#ifndef HPP_GUARD_PLEXUS_DATAGRAM_MTU_BUDGET_H
#define HPP_GUARD_PLEXUS_DATAGRAM_MTU_BUDGET_H

#include <cstddef>

namespace plexus::datagram {

// A static per-channel per-datagram payload budget the fragment/oversize path consults: a payload
// larger than this is split across numbered datagrams rather than rejected, so it caps a single
// datagram's bytes, not a logical message. There is no path-MTU discovery (no ICMP coupling). The
// default is the RFC 9000 §14 conservative datagram bound: a tunnel/VPN path below the fragment
// size blackholes silently, so 1200 is the safe floor an arbitrary IP path always carries.
// field_payload_ceiling is the opt-in jumbo-class knob a caller on a known wide path passes.
struct mtu_budget
{
    static constexpr std::size_t field_payload_ceiling = 8192;

    std::size_t max_payload = 1200;
};

}

#endif
