#ifndef HPP_GUARD_PLEXUS_IO_RELIABILITY_H
#define HPP_GUARD_PLEXUS_IO_RELIABILITY_H

#include <cstdint>

namespace plexus::io {

// The delivery-guarantee axis: a single-valued choice naming WHETHER a topic's
// datagrams are delivered best-effort (lossy, fire-and-forget — the UDP class)
// or reliably (in-order, retransmitted until acked — the TCP-equivalent class).
// Unlike locality this is NOT a composable mask: a topic is one class or the
// other, never a union, so there are no bitwise operators here. best_effort is
// the lossy default; reliable opts into the ack/retransmit machinery.
enum class reliability : std::uint8_t
{
    best_effort,
    reliable,
};

}

#endif
