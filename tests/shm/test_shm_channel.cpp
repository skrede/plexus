#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/loan_status.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/shm_channel.h"
#include "plexus/io/shm/shm_slot_owner.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

// shm_channel send -> drain round-trip over an anonymous mapped span: a payload
// loans, one memcpy fills the slab, publish commits, the notifier signals exactly
// once per successful send and zero times on a reject. drain hands the bytes back
// zero-copy (wire_bytes<shm_slot_owner>) byte-equal. The oversize case proves the
// fallback: a payload over the slot capacity returns rejected, nothing lands, the
// notifier is never signaled.

using namespace plexus::io::shm;

namespace {

// A recording stub notifier satisfying the core notifier concept: it counts
// signal() calls and remembers whether it is armed, with no kernel object (the
// channel/registry never touches an asio/futex type in this test TU).
struct recording_notifier
{
    std::uint32_t signals = 0;
    bool          armed   = false;

    void signal() noexcept { ++signals; }
    void arm(plexus::detail::move_only_function<void()>) { armed = true; }
    void disarm() noexcept { armed = false; }
};

static_assert(notifier<recording_notifier>, "the recording stub must satisfy the notifier seam");

struct backing_region
{
    explicit backing_region(std::size_t bytes)
        : m_storage(bytes + k_cache_line)
    {
        auto base    = reinterpret_cast<std::uintptr_t>(m_storage.data());
        auto aligned = (base + k_cache_line - 1) & ~static_cast<std::uintptr_t>(k_cache_line - 1);
        m_data       = reinterpret_cast<std::byte *>(aligned);
        m_size       = bytes;
    }
    std::span<std::byte> span() const noexcept { return {m_data, m_size}; }

private:
    std::vector<std::byte> m_storage;
    std::byte             *m_data{nullptr};
    std::size_t            m_size{0};
};

struct fixture
{
    static constexpr std::uint64_t k_cells = 64;
    static constexpr std::uint64_t k_slot  = 64;

    backing_region     control{control_region_bytes(k_cells)};
    backing_region     slab{slab_region_bytes(k_cells, k_slot)};
    broadcast_ring     ring;
    recording_notifier notify;

    fixture()
    {
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, ring) ==
                loan_status::ok);
    }
};

}

TEST_CASE("shm.channel send->drain round-trips a payload, signaling once per send", "[shm][channel]")
{
    fixture f;
    // One channel composes the publisher + a subscriber over the ring; its own
    // subscriber registers a cursor at the tail, so a send it commits is drained
    // back through the same channel.
    shm_channel<recording_notifier> channel(f.ring, f.notify, plexus::io::reliability::reliable,
                                            plexus::io::congestion::block);

    constexpr int k_iterations = 200;
    for(int i = 0; i < k_iterations; ++i)
    {
        const std::uint32_t value = 0xA5A50000u | static_cast<std::uint32_t>(i & 0xffff);
        std::byte           payload[sizeof(value)];
        std::memcpy(payload, &value, sizeof(value));

        REQUIRE(channel.send(std::span<const std::byte>(payload, sizeof(payload))) ==
                loan_status::ok);

        std::uint32_t delivered = 0;
        int           count     = 0;
        shm_channel<recording_notifier>::deliver_fn deliver =
            [&](plexus::wire_bytes<shm_slot_owner> wb) {
                REQUIRE(wb.size() == sizeof(value));
                std::memcpy(&delivered, wb.data(), sizeof(delivered));
                ++count;
            };
        channel.drain(deliver);

        REQUIRE(count == 1);
        REQUIRE(delivered == value);
    }

    // The notifier fired exactly once per successful send -- never more, never less.
    REQUIRE(f.notify.signals == static_cast<std::uint32_t>(k_iterations));
}

TEST_CASE("shm.oversize a payload over the slot capacity returns rejected without signaling",
          "[shm][oversize]")
{
    fixture f;
    shm_channel<recording_notifier> channel(f.ring, f.notify, plexus::io::reliability::reliable,
                                            plexus::io::congestion::block);

    // A payload one byte over the slot capacity: send() must reject it, with NO
    // publish landing and NO notifier signal -- the oversize fallback, not a silent
    // drop.
    std::vector<std::byte> oversize(fixture::k_slot + 1, std::byte{0x7f});
    REQUIRE(channel.send(std::span<const std::byte>(oversize.data(), oversize.size())) ==
            loan_status::rejected);

    REQUIRE(f.notify.signals == 0u); // the reject never signaled

    // No message landed: a drain delivers nothing.
    int                                         delivered = 0;
    shm_channel<recording_notifier>::deliver_fn deliver =
        [&](plexus::wire_bytes<shm_slot_owner>) { ++delivered; };
    channel.drain(deliver);
    REQUIRE(delivered == 0);

    // A subsequent in-bounds send still works (the reject left the ring clean).
    std::vector<std::byte> ok_payload(fixture::k_slot, std::byte{0x11});
    REQUIRE(channel.send(std::span<const std::byte>(ok_payload.data(), ok_payload.size())) ==
            loan_status::ok);
    REQUIRE(f.notify.signals == 1u);
}
