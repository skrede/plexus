#ifndef HPP_GUARD_PLEXUS_IO_MTU_BUDGET_H
#define HPP_GUARD_PLEXUS_IO_MTU_BUDGET_H

#include <cstddef>

namespace plexus::io {

// A conservative static per-channel payload budget that the segmentation /
// oversize-reject path consults: a frame whose enveloped size exceeds
// max_payload is rejected at publish rather than fragmented or silently
// dropped. The budget is deterministic — there is no path-MTU discovery (which
// would couple the data plane to ICMP and a moving network state), so the value
// is fixed at setup and friendly to an MCU / lwIP profile. A caller MAY override
// the default; the default is a behavior-preserving floor that fits inside a
// single conventional Ethernet datagram without relying on IP fragmentation.
struct mtu_budget
{
    std::size_t max_payload = 1400;
};

}

#endif
