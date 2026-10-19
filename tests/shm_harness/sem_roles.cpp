#include "sem_support.h"
#include "region_doorbells.h"

#include "support/heap_ring.h"
#include "support/region_names.h"
#include "support/xproc_harness.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/slot_subscriber.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/same_host_geometry.h"

#include <asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>
#include <iostream>

#include <semaphore.h>

#if defined(__linux__)
    #include <csignal>

    #include <sys/prctl.h>
#endif

namespace {

using namespace shm_harness;
using namespace std::chrono_literals;

constexpr std::uint32_t k_arm_cycles    = 100;
constexpr std::uint32_t k_flood_signals = 100'000;

constexpr plexus::shm::ring_direction k_request = plexus::shm::ring_direction::request;

void settle(::asio::io_context &io, std::chrono::milliseconds span)
{
    io.restart();
    io.run_for(span);
}

// The first cycle settles so this process's waiter parks while the peer's waiter is parked on the same ring.
bool sem_cycler(const std::vector<std::string> &argv)
{
#if defined(__linux__)
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
    if(argv.size() != 1)
        return false;

    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    sem_registry registry{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, posix_sem_leg::binder(io)};
    for(std::uint32_t cycle = 0; cycle < k_arm_cycles; ++cycle)
    {
        if(registry.acquire(argv[0], k_request, plexus::testing::test_publisher(), plexus::shm::ring_role::consumer, 0) == plexus::shm::acquire_result::failed)
            return false;
        settle(io, cycle == 0 ? 200ms : 1ms);
        registry.release(argv[0], k_request, plexus::testing::test_publisher(), plexus::shm::ring_role::consumer);
    }
    return true;
}

// The waiter stops consuming its semaphore's count only once it blocks, so a count unchanged across a settle means the
// waiter is either blocked or idle. macOS does not implement sem_getvalue, so there a fixed settle stands in.
void wait_until_waiter_settles(const std::string &sem_name)
{
    sem_t *sem   = ::sem_open(sem_name.c_str(), 0);
    int current  = -1;
    int previous = -1;
    if(sem == SEM_FAILED || ::sem_getvalue(sem, &current) != 0)
    {
        if(sem != SEM_FAILED)
            ::sem_close(sem);
        std::this_thread::sleep_for(1s);
        return;
    }
    for(std::uint32_t poll = 0; poll < 25 && current != previous; ++poll)
    {
        previous = current;
        std::this_thread::sleep_for(200ms);
        ::sem_getvalue(sem, &current);
    }
    ::sem_close(sem);
    std::cout << "sem_flood settled count=" << current << std::endl;
}

// The reactor is never polled, so every wake the waiter relays stays pending in its wake pipe.
bool sem_flood(const std::vector<std::string> &argv)
{
#if defined(__linux__)
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
    if(argv.size() != 1)
        return false;

    plexus::testing::heap_ring heap{16, 64};
    plexus::shm::slot_subscriber subscriber{heap.ring()};
    ::asio::io_context io;
    const std::string region = plexus::testing::region_name(argv[0], plexus::testing::test_publisher());
    sem_notifier notifier{io, heap.ring().header(), region};
    const std::uint32_t slot = subscriber.cursor_index().value_or(0);
    if(!heap.created() || !subscriber.cursor_index() || notifier.arm(slot, [] {}) != plexus::shm::arm_status::armed)
        return false;

    std::thread signaler(
            [&notifier]
            {
                for(std::uint32_t signal = 0; signal < k_flood_signals; ++signal)
                    notifier.signal();
            });
    signaler.join();
    wait_until_waiter_settles(sem_name_for(region, slot, heap.ring().header().cursors[slot].waiter_tag.load(std::memory_order_acquire)));
    notifier.disarm();
    return true;
}

[[maybe_unused]] const bool s_cycler_registered = plexus::testing::register_xproc_child("shm_harness.sem_cycler", &sem_cycler);
[[maybe_unused]] const bool s_flood_registered  = plexus::testing::register_xproc_child("shm_harness.sem_flood", &sem_flood);

}
