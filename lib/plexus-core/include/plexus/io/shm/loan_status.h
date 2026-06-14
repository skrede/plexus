#ifndef HPP_GUARD_PLEXUS_IO_SHM_LOAN_STATUS_H
#define HPP_GUARD_PLEXUS_IO_SHM_LOAN_STATUS_H

#include <cstdint>

namespace plexus::io::shm {

// Synchronous status returned by value from every loan/publish/take verb; the
// produced handle (a loaned_buffer or taken_message) comes back through an
// out-param. Leads with ok exactly as plexus::io::io_error does, so the status
// families read identically at every call site. No exceptions, no std::expected.
//
// Per-verb status subsets:
//   loan    -> { ok, rejected (size exceeds the slot capacity),
//                congested (no free slot under the reliable policy) }
//   publish -> { ok }
//   consume -> { ok, empty, congested, lagged }
//   take    -> { ok, empty }
//
// `empty` is the would-block alias for take(): take() returning empty means the
// consumer has nothing new to read for its cursor, not an error.
//
// `lagged` means a best-effort consumer's cursor fell a full ring behind the
// producer (the producer lapped it): consume() carries the producer tail in the
// out-param so the consumer can jump its cursor there in one step rather than
// stepping a slot at a time. take() resolves it internally, so a take() caller
// only ever sees ok/empty.
//
// Note: with the fixed-stride payload slab the cell -- and therefore the payload
// slot -- is claimed at loan() time, so `congested` is a loan() return rather
// than a publish() return. publish() only commits a slot already owned by the
// caller and so cannot become congested.
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
