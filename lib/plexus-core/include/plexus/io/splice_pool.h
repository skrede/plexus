#ifndef HPP_GUARD_PLEXUS_IO_SPLICE_POOL_H
#define HPP_GUARD_PLEXUS_IO_SPLICE_POOL_H

#include "plexus/io/detail/drop_event.h"

#include "plexus/wire_bytes.h"

#include <span>
#include <limits>
#include <vector>
#include <memory>
#include <cstddef>
#include <utility>

namespace plexus::io {

// A fixed-capacity, grow-once pool of pre-sized byte slots feeding the relay splice. Each slot is owned
// once at construction behind a shared_ptr; a checkout hands back an aliasing wire_bytes<> view sharing
// that control block (no per-checkout allocation), so a slot is free exactly when the pool holds the
// only reference to it. Exhaustion never blocks and never grows: the owned-copy checkout counts a drop
// and returns an empty wire_bytes the caller degrades on. Single-threaded per receive turn — the free
// scan reads use_count without synchronization because no other turn shares the pool concurrently.
class splice_pool
{
public:
    splice_pool(std::size_t slot_count, std::size_t slot_bytes)
    {
        m_slots.reserve(slot_count);
        for(std::size_t i = 0; i < slot_count; ++i)
            m_slots.push_back(std::make_shared<std::vector<std::byte>>(slot_bytes));
    }

    // Pooled owned copy (D95.1 default): claim a free slot, let enc(std::span<std::byte>) write the
    // forwarded envelope into it and return the byte count, then hand back a wire_bytes<> whose owner
    // returns the slot to the pool on final release. On exhaustion the resident set is untouched, the
    // drop is counted, and an empty wire_bytes degrades the caller to drop-with-count.
    template<typename Encode>
    wire_bytes<> checkout_owned_copy(Encode &&enc)
    {
        const std::size_t i = claim_free_slot();
        if(i == npos)
        {
            ++m_exhaustion_drops;
            return wire_bytes<>{};
        }
        auto &slot                = m_slots[i];
        const std::size_t written = enc(std::span<std::byte>{*slot});
        std::span<const std::byte> view{slot->data(), written};
        return wire_bytes<>{view, std::shared_ptr<const void>{slot, slot->data()}};
    }

    // The minimal refcounted zero-copy opt-in seam: wrap a caller-supplied owner-carrying shared_bytes
    // via the wire_bytes converting ctor, retaining the owner with NO slot copy. The owner-carrying
    // receive path that feeds this seam is plumbed behind the same knob one layer up; here the seam is
    // unit-exercisable with a synthesized owner. Never yields a bare span with an empty owner.
    static wire_bytes<> checkout_zero_copy(const wire::shared_bytes &owner) noexcept
    {
        return wire_bytes<>{owner};
    }

    std::size_t slot_count() const noexcept
    {
        return m_slots.size();
    }
    std::uint64_t exhaustion_drops() const noexcept
    {
        return m_exhaustion_drops;
    }
    static constexpr detail::drop_cause exhaustion_cause() noexcept
    {
        return detail::drop_cause::splice_exhausted;
    }

private:
    static constexpr std::size_t npos = std::numeric_limits<std::size_t>::max();

    // A slot is free when the pool holds its sole reference; a live checkout (and any downstream
    // addref-share) keeps the use_count above one until the send completes.
    std::size_t claim_free_slot() const noexcept
    {
        for(std::size_t i = 0; i < m_slots.size(); ++i)
            if(m_slots[i].use_count() == 1)
                return i;
        return npos;
    }

    std::vector<std::shared_ptr<std::vector<std::byte>>> m_slots;
    std::uint64_t m_exhaustion_drops{0};
};

}

#endif
