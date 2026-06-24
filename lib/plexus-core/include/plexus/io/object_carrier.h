#ifndef HPP_GUARD_PLEXUS_IO_OBJECT_CARRIER_H
#define HPP_GUARD_PLEXUS_IO_OBJECT_CARRIER_H

#include <cstdint>

namespace plexus::io {

struct loan_slot
{
    const void *object;
    std::uint32_t refs;
    void (*release)(loan_slot *);
};

// A native_key mismatch under a matching type_tag is a COUNTED DROP at the demux,
// never a cast: a forged wire tag that collides cannot select a wrong-type C++
// reinterpretation. The refcount is non-atomic — sound because every channel on one
// inproc bus shares a single executor and delivery happens ONLY from its step loop,
// so addref at enqueue and release at delivery/drop never race across threads.
struct object_carrier
{
    std::uint64_t topic_hash;
    std::uint64_t type_tag;
    const void *native_key;
    std::uint64_t sequence;
    std::uint64_t source_timestamp;
    loan_slot *slot;
};

// The address of this per-T inline constant is the native_key a producer stamps and
// a receiving demux compares.
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
