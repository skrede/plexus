#include "plexus/asio/shm/linux/ring_notifier.h"

#include "plexus/asio/asio_policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <atomic>
#include <cstdint>

// The liburing usage-requirement proof: this TU drives the io_uring notifier's arm/signal/disarm,
// which call io_uring_queue_init / io_uring_register_eventfd / io_uring_submit /
// io_uring_queue_exit directly. The target links ONLY plexus::asio::shm (+ plexus::asio) and
// NO manual liburing — so a green link here proves plexus::asio::shm carries liburing as a
// usage requirement (the symptom a real consumer hit: undefined reference to io_uring_*). Only
// meaningful where liburing exposes futex-wait and ring_notifier is the io_uring bridge; the
// threaded floor uses no io_uring, so there is nothing to prove there.

#if PLEXUS_HAS_IO_URING_FUTEX
TEST_CASE("shm.liburing_link plexus::asio::shm transitively carries liburing", "[shm][link]")
{
    ::asio::io_context io;
    std::atomic<std::uint32_t> word{0};
    std::atomic<std::uint32_t> park{0};

    plexus::asio::shm::ring_notifier<plexus::asio::asio_policy> notifier{io, word, park};

    bool drained = false;
    notifier.arm([&] { drained = true; });
    notifier.signal();
    io.poll();
    notifier.disarm();

    SUCCEED("ring_notifier linked and ran with no manually-added liburing");
}
#else
TEST_CASE("shm.liburing_link io_uring futex-wait unavailable, threaded floor in use", "[shm][link]")
{
    SUCCEED("liburing lacks io_uring_prep_futex_wait; ring_notifier is the threaded floor");
}
#endif
