#ifndef HPP_GUARD_PLEXUS_IO_OBJECT_CARRIER_H
#define HPP_GUARD_PLEXUS_IO_OBJECT_CARRIER_H

#include <cstdint>

namespace plexus::io {

// The intrusive control block a published object handle rides on. The producer's
// pool owns the backing slot; refs counts the live in-flight references the lane
// holds, and release(this) returns the slot to its pool when refs reaches zero.
// release is a plain function pointer (a per-T static, never a virtual) so the
// slot carries no vtable and the producer's concrete type is recovered by the
// release function, not by a cast here.
struct loan_slot
{
    const void   *object;
    std::uint32_t refs;
    void (*release)(loan_slot *);
};

// The type-erased handle the process-tier fast path moves between channels with
// zero serialization. A plain POD assembled at the publishing forwarder — it owns
// no payload; the bytes live in the producer pool's slot, kept alive by the
// loan_slot refcount for the duration the carrier is in flight.
//
// The type identity is DUAL on purpose. type_tag is the wire/contract identity (the
// same value a subscriber declares and the producer matches at attach) and is the
// ONLY thing that gates fast-path eligibility. native_key is a process-local C++
// witness (the address of a per-T inline constant): a tag match with a key mismatch
// is a COUNTED DROP at the receiving demux, never a cast — so a forged wire tag that
// happens to collide can never select a wrong-type C++ reinterpretation.
//
// The refcount is non-atomic. This is sound because every channel on one inproc bus
// shares a single executor and delivery happens ONLY from that executor's step loop
// (the bus's posted-only discipline) — addref at enqueue and release at
// delivery/drop never race across threads.
struct object_carrier
{
    std::uint64_t topic_hash;
    std::uint64_t type_tag;
    const void   *native_key;
    std::uint64_t sequence;
    std::uint64_t source_timestamp;
    loan_slot    *slot;
};

// The per-T process-local witness: the address of this inline constant is the
// native_key a producer stamps and a receiving demux compares. A constant, not a
// singleton — it holds no mutable state, so the no-static-singletons discipline does
// not apply (it is a type witness whose identity is its address).
namespace detail {
template<typename T>
inline constexpr char type_key = 0;
}

inline void addref(const object_carrier &c) noexcept
{
    if(c.slot)
        ++c.slot->refs;
}

inline void release(const object_carrier &c) noexcept
{
    if(c.slot && --c.slot->refs == 0)
        c.slot->release(c.slot);
}

}

#endif
