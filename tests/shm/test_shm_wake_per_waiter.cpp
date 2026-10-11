#include "support/registered_waiters.h"

#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/waiter_park.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <cstdint>

namespace {

using namespace std::chrono_literals;

// A waiter thread parked in its own slot; destruction notifies and wakes that slot before the join, so the
// thread is released whatever the test observed.
class blocked_waiter
{
public:
    blocked_waiter(plexus::shm::control_header_t &header, std::uint32_t slot)
            : m_slot(slot)
            , m_entered(false)
            , m_returned(false)
            , m_header(header)
            , m_thread([this] { wait(); })
    {
        while(!m_entered.load(std::memory_order_acquire))
            std::this_thread::yield();
        std::this_thread::sleep_for(20ms);
    }

    blocked_waiter(const blocked_waiter &)            = delete;
    blocked_waiter &operator=(const blocked_waiter &) = delete;

    ~blocked_waiter()
    {
        plexus::shm::park_word(m_header, m_slot).store(plexus::shm::k_park_notified);
        plexus::native::notifier_wake(plexus::shm::park_word(m_header, m_slot));
        m_thread.join();
    }

    bool returned_within(std::chrono::milliseconds bound) const
    {
        const auto deadline = std::chrono::steady_clock::now() + bound;
        while(!m_returned.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(1ms);
        return m_returned.load(std::memory_order_acquire);
    }

private:
    std::uint32_t m_slot;
    std::atomic<bool> m_entered;
    std::atomic<bool> m_returned;
    plexus::shm::control_header_t &m_header;
    std::thread m_thread;

    void wait()
    {
        std::atomic<std::uint32_t> &park = plexus::shm::park_word(m_header, m_slot);
        const std::uint32_t snapshot     = plexus::shm::park_snapshot(m_header);
        plexus::shm::park_store(m_header, m_slot);
        m_entered.store(true, std::memory_order_release);
        while(plexus::shm::park_recheck(m_header, snapshot) && park.load() == plexus::shm::k_park_parked)
            plexus::native::notifier_wait(park, plexus::shm::k_park_parked);
        m_returned.store(true, std::memory_order_release);
    }
};

}

// Waiter A parks and blocks; waiter B parks and unparks in its own slot; the signal must still find A parked.
TEST_CASE("wake_per_waiter a parked waiter is woken after another waiter unparks", "[shm][wake_per_waiter]")
{
    plexus::shm::control_header_t header{};
    plexus::testing::register_waiters(header, 2);
    const blocked_waiter a{header, 0};

    plexus::shm::park_store(header, 1);
    plexus::shm::park_clear(header, 1);
    plexus::native::notifier_signal(header);

    REQUIRE(a.returned_within(200ms));
}
