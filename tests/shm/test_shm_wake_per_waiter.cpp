#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/ring_layout.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <cstdint>

namespace {

using namespace std::chrono_literals;

// A waiter thread blocked in notifier_wait on the generation it snapshotted; destruction bumps and wakes every waiter
// before the join, so the thread is released whatever the test observed.
class blocked_waiter
{
public:
    explicit blocked_waiter(plexus::shm::control_header_t &header)
            : m_entered(false)
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
        plexus::native::notifier_signal(m_header.notify_generation);
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
    std::atomic<bool> m_entered;
    std::atomic<bool> m_returned;
    plexus::shm::control_header_t &m_header;
    std::thread m_thread;

    void wait()
    {
        const std::uint32_t snapshot = m_header.notify_generation.load(std::memory_order_acquire);
        m_header.park_state.store(plexus::shm::k_park_parked, std::memory_order_release);
        m_entered.store(true, std::memory_order_release);
        plexus::native::notifier_wait(m_header.notify_generation, snapshot);
        m_returned.store(true, std::memory_order_release);
    }
};

}

// Waiter A parks and blocks; waiter B, sharing A's park word, unparks; the gated signal then finds the word EMPTY.
TEST_CASE("wake_per_waiter a parked waiter is woken after another waiter unparks", "[shm][wake_per_waiter][!shouldfail]")
{
    plexus::shm::control_header_t header{};
    const blocked_waiter a{header};

    header.park_state.store(plexus::shm::k_park_empty, std::memory_order_release);
    plexus::native::notifier_signal(header.notify_generation, header.park_state);

    REQUIRE(a.returned_within(200ms));
}
