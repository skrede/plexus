#include "support/xproc_harness.h"

#include "plexus/shm/shm_backend_version.h"

#include <catch2/catch_test_macros.hpp>

#include <sys/mman.h>

#include <atomic>
#include <cstdint>

// The behavioral-oracle placeholder the later waves fill with the real ring /
// notifier xproc cases. It does NOT implement any SHM transport logic — it only
// proves two host facts the whole phase rests on: (1) the gated plexus::shm
// target links and runs (backend_version is a real linked symbol), and (2) a
// forked child and its parent actually share an anonymous MAP_SHARED page so a
// value written by the child is observed by the parent across the address-space
// boundary. Looped N>=100 per feedback_no_success_from_single_run.

using namespace plexus::testing;

TEST_CASE("scaffold: the gated shm backend links and runs", "[shm][scaffold]")
{
    REQUIRE(plexus::shm::backend_version() == "0.1.0");
}

TEST_CASE("scaffold: a value round-trips through MAP_SHARED across fork", "[shm][scaffold]")
{
    constexpr int k_iterations = 128;
    for(int i = 0; i < k_iterations; ++i)
    {
        auto *word = static_cast<std::atomic<std::uint32_t> *>(
                ::mmap(nullptr, sizeof(std::atomic<std::uint32_t>), PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        REQUIRE(word != MAP_FAILED);
        new(word) std::atomic<std::uint32_t>{0};

        const std::uint32_t expected = 0xA5A50000u | static_cast<std::uint32_t>(i);

        const xproc_outcome outcome =
                run_forked([&] { return word->load(std::memory_order_acquire) == expected; },
                           [&]
                           {
                               word->store(expected, std::memory_order_release);
                               return true;
                           });

        const std::uint32_t observed = word->load(std::memory_order_acquire);
        ::munmap(word, sizeof(std::atomic<std::uint32_t>));

        REQUIRE(outcome.child_succeeded);
        REQUIRE(observed == expected);
    }
}
