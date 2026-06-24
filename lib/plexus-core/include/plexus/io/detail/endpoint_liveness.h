#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ENDPOINT_LIVENESS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ENDPOINT_LIVENESS_H

#include <cstdint>

namespace plexus::io::detail {

// last_data_seen_ns is stamped by a DATA frame only; last_seen_ns by a DATA frame or a
// presence heartbeat. A heartbeat refreshes last_seen_ns but NOT last_data_seen_ns, so it
// keeps a lease alive without masking a genuine deadline lapse. A period of 0 means the axis
// was not requested: it stays inert. The *_violated/*_expired flags are edge latches — fire
// once per lapse, re-arm only when the gap falls back under the period.
struct endpoint_liveness
{
    std::uint64_t last_data_seen_ns  = 0;
    std::uint64_t last_seen_ns       = 0;
    std::uint64_t deadline_period_ns = 0;
    std::uint64_t lease_ns           = 0;
    bool deadline_violated           = false;
    bool lease_expired               = false;
};

}

#endif
