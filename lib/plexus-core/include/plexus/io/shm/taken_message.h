#ifndef HPP_GUARD_PLEXUS_IO_SHM_TAKEN_MESSAGE_H
#define HPP_GUARD_PLEXUS_IO_SHM_TAKEN_MESSAGE_H

#include "plexus/io/shm/shm_slot_owner.h"

#include "plexus/wire_bytes.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>

namespace plexus::io::shm {

class slot_subscriber;

namespace test {
struct handle_test_access;
}

// Move-only, read-only consumer handle. bytes() aliases the slab slot the take
// pinned; the take_refcount keeps the slot from being recycled while this handle
// (or a wire_bytes aliasing it) is alive.
//
// Lifetime invariant: release decrements take_refcount EXACTLY ONCE. reclaim() is
// idempotent -- it decrements once then nulls the back-pointer so a moved-from
// handle's destructor and a move-assign target both no-op.
//
// as_wire_bytes() is the zero-copy read seam (the move-only owner reshape): it builds a
// plexus::wire_bytes<shm_slot_owner> whose move-only intrusive owner pins the SAME
// take_refcount, so the slot stays pinned for the deserialized view's lifetime
// independently of this handle. There is no reference-counted heap handle
// anywhere on this path -- the owner is a bare intrusive refcount pointer. An
// 8-aligned slot base plus that owner is exactly the alias precondition a codec's
// deserialize takes instead of the copy-to-align fallback.
class taken_message
{
public:
    taken_message() = default;

    ~taken_message() { reclaim(); }

    taken_message(const taken_message &)            = delete;
    taken_message &operator=(const taken_message &) = delete;

    taken_message(taken_message &&other) noexcept
            : m_payload(other.m_payload)
            , m_length(other.m_length)
            , m_refcount(other.m_refcount)
    {
        other.m_payload  = nullptr;
        other.m_length   = 0;
        other.m_refcount = nullptr;
    }

    taken_message &operator=(taken_message &&other) noexcept
    {
        if(this == &other)
            return *this;

        reclaim();

        m_payload  = other.m_payload;
        m_length   = other.m_length;
        m_refcount = other.m_refcount;

        other.m_payload  = nullptr;
        other.m_length   = 0;
        other.m_refcount = nullptr;
        return *this;
    }

    // Read-only view aliasing the slab slot. Calling this on an empty or moved-from
    // handle is caller misuse (assert).
    std::span<const std::byte> bytes() const
    {
        assert(m_payload != nullptr && "taken_message::bytes() on an empty or moved-from handle");
        return {m_payload, m_length};
    }

    // A wire_bytes aliasing the slot, owner-pinned via a move-only shm_slot_owner.
    // It takes a FRESH pin so the returned view's lifetime is independent of this
    // handle. Calling this on an empty or moved-from handle is caller misuse.
    ::plexus::wire_bytes<shm_slot_owner> as_wire_bytes() const
    {
        assert(m_payload != nullptr &&
               "taken_message::as_wire_bytes() on an empty or moved-from handle");
        return ::plexus::wire_bytes<shm_slot_owner>(std::span<const std::byte>(m_payload, m_length),
                                                    shm_slot_owner(m_refcount));
    }

    // The adopt tag: the slot_subscriber has ALREADY pinned the slot via the ring's
    // Dekker-safe pin_if_current (the seq_cst announce + recheck the best_effort
    // overwrite race needs). This ctor ADOPTS that existing pin rather than adding a
    // second one (the carry-forward "must not double-pin" rule) -- the handle owns
    // the one held pin and releases it exactly once on reclaim().
    struct adopt_pin_t
    {
    };
    static constexpr adopt_pin_t adopt_pin{};

private:
    friend class slot_subscriber;
    friend struct ::plexus::io::shm::test::handle_test_access;

    taken_message(const std::byte *payload, std::size_t length,
                  std::atomic<std::uint32_t> *refcount) noexcept
            : m_payload(payload)
            , m_length(length)
            , m_refcount(refcount)
    {
        if(m_refcount != nullptr)
            m_refcount->fetch_add(1, std::memory_order_acq_rel); // pin at take()
    }

    // Adopting ctor: takes ownership of a pin the caller already holds (no
    // fetch_add). reclaim() still decrements once, so the held pin is released
    // exactly once when the handle dies.
    taken_message(adopt_pin_t, const std::byte *payload, std::size_t length,
                  std::atomic<std::uint32_t> *refcount) noexcept
            : m_payload(payload)
            , m_length(length)
            , m_refcount(refcount)
    {
    }

    // Decrements take_refcount once then nulls the back-pointer. noexcept +
    // idempotent so the move/destructor paths release exactly once.
    void reclaim() noexcept
    {
        if(m_refcount != nullptr)
            m_refcount->fetch_sub(1, std::memory_order_acq_rel);

        m_payload  = nullptr;
        m_length   = 0;
        m_refcount = nullptr;
    }

    const std::byte            *m_payload{nullptr};
    std::size_t                 m_length{0};
    std::atomic<std::uint32_t> *m_refcount{nullptr};
};

}

#endif
