#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ENDPOINT_LIVENESS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ENDPOINT_LIVENESS_H

#include <cstdint>

namespace plexus::io::detail {

// The per-endpoint liveness state record the periodic check scans. It is a plain
// trivially-copyable value grown ONCE at the register seam (cold), then mutated on
// the receive path by a plain store into a resident field — never an allocation,
// never a timer arm.
//
// Two distinct clocks live here so a silent-but-alive publisher cannot mask a real
// deadline miss:
//   - last_data_seen_ns is stamped by a DATA frame only (the deadline clock): the
//     inter-data gap drives the missed-deadline check.
//   - last_seen_ns is stamped by a DATA frame OR a presence heartbeat (the
//     liveliness clock): the inter-presence gap drives the lease-expiry check.
// A heartbeat refreshes last_seen_ns but NOT last_data_seen_ns, so it keeps a lease
// alive without clearing a genuine deadline lapse.
//
// deadline_period_ns and lease_ns are the subscriber's OWN requested periods, copied
// in at register. A value of 0 is a genuine "not requested" absence: that axis is
// inert and never participates in the scan (it never false-fires).
//
// deadline_violated and lease_expired are edge latches: the check fires the event
// exactly ONCE per lapse and re-arms only when presence/data resumes (a stamp that
// brings the gap back under the period clears the latch).
struct endpoint_liveness
{
    std::uint64_t last_data_seen_ns = 0;
    std::uint64_t last_seen_ns      = 0;
    std::uint64_t deadline_period_ns = 0;
    std::uint64_t lease_ns           = 0;
    bool deadline_violated = false;
    bool lease_expired     = false;
};

}

#endif
