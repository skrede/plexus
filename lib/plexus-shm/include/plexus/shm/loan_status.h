#ifndef HPP_GUARD_PLEXUS_SHM_LOAN_STATUS_H
#define HPP_GUARD_PLEXUS_SHM_LOAN_STATUS_H

#include <cstdint>

namespace plexus::shm {

// Per-verb status subsets:
//   loan    -> { ok, rejected (size exceeds the slot capacity), congested }
//   publish -> { ok }
//   consume -> { ok, empty, congested, lagged }
//   take    -> { ok, empty }
// `empty` is the would-block alias for take(). `lagged` means the consumer's
// cursor fell a full ring behind the producer; consume() carries the producer
// tail in the out-param so the caller jumps its cursor there in one step (take()
// resolves it internally, so a take() caller only ever sees ok/empty). The slot
// is claimed at loan() time, so `congested` is a loan() return, never publish().
enum class loan_status : std::uint8_t
{
    ok,
    congested,
    rejected,
    empty,
    lagged,
    unknown
};

}

#endif
