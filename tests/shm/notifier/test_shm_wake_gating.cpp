#include "support/registered_waiters.h"

#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/waiter_park.h"

#include <catch2/catch_test_macros.hpp>

#include <new>
#include <atomic>
#include <cstdint>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

namespace {

using namespace plexus::shm;
using plexus::testing::register_waiters;

struct shared_state
{
    control_header_t header;
    std::atomic<std::uint32_t> child_parked;
};

shared_state *map_shared_state()
{
    void *p = ::mmap(nullptr, sizeof(shared_state), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if(p == MAP_FAILED)
        return nullptr;
    shared_state *s = ::new(p) shared_state{};
    register_waiters(s->header, 1);
    return s;
}

void park_and_wait(shared_state &s)
{
    const std::uint32_t snapshot = park_snapshot(s.header);
    park_store(s.header, 0);
    s.child_parked.store(1, std::memory_order_release);
    if(!park_recheck(s.header, snapshot))
        return;
    while(park_word(s.header, 0).load(std::memory_order_acquire) == k_park_parked)
        plexus::native::notifier_wait(park_word(s.header, 0), k_park_parked);
}

}

TEST_CASE("shm.wake_gating only a parked waiter's slot asks for a wake", "[shm][wake_gating]")
{
    control_header_t header{};
    register_waiters(header, 2);

    REQUIRE(signal_parked(header) == 0);
    REQUIRE(park_word(header, 0).load() == k_park_empty);

    park_word(header, 0).store(k_park_notified);
    REQUIRE(signal_parked(header) == 0);

    park_store(header, 1);
    REQUIRE(signal_parked(header) == 0b10u);
    REQUIRE(park_word(header, 1).load() == k_park_notified);
}

TEST_CASE("shm.wake_gating an unregistered slot is never woken", "[shm][wake_gating]")
{
    control_header_t header{};
    register_waiters(header, 2);
    header.cursors[1].active.store(0);
    park_store(header, 1);

    REQUIRE(signal_parked(header) == 0);
    REQUIRE(park_word(header, 1).load() == k_park_parked);
}

TEST_CASE("shm.wake_gating a rewritten capacity never scans past the cursor array", "[shm][wake_gating]")
{
    control_header_t header{};
    register_waiters(header, static_cast<std::uint32_t>(k_max_consumers));
    header.consumer_capacity = 1000;
    header.high_water.store(1000);
    park_store(header, k_max_consumers - 1);

    REQUIRE(signal_parked(header) == std::uint32_t{1} << (k_max_consumers - 1));
}

TEST_CASE("shm.wake_gating a parked waiter is woken across address spaces", "[shm][wake_gating]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        shared_state *s = map_shared_state();
        REQUIRE(s != nullptr);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);
        if(pid == 0)
        {
            park_and_wait(*s);
            ::_exit(0);
        }

        while(s->child_parked.load(std::memory_order_acquire) == 0)
            ;
        plexus::native::notifier_signal(s->header);

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0)
            ;
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);

        ::munmap(s, sizeof(shared_state));
    }
}
