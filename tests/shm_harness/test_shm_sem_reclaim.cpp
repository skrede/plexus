#include "sem_support.h"
#include "topic_regions.h"
#include "launch_helpers.h"
#include "region_doorbells.h"

#include "region_names.h"
#include "xproc_control.h"
#include "xproc_harness.h"
#include "xproc_process.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/testing/platform.h"

#include <asio/io_context.hpp>

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <string>
#include <cstdint>
#include <optional>
#include <string_view>

#include <fcntl.h>
#include <semaphore.h>

namespace {

using namespace shm_harness;
using namespace std::chrono_literals;

constexpr std::uint32_t k_sends                 = 512; // twice the ring's depth for a small payload
constexpr plexus::shm::ring_direction k_request = plexus::shm::ring_direction::request;

// The errno of opening the semaphore by name, 0 when it opens.
std::int32_t open_error(const std::string &name)
{
    sem_t *sem = ::sem_open(name.c_str(), 0);
    if(sem == SEM_FAILED)
        return errno;
    ::sem_close(sem);
    return 0;
}

// This process produces into a reliable blocking ring on the semaphore notifier; a child arms a reader on it and parks.
struct reclaim_fixture
{
    explicit reclaim_fixture(std::string_view tag)
            : fqn("shm_harness.sem_reclaim." + std::string(tag) + "." + std::to_string(plexus::testing::process_id()))
            , regions{fqn, {plexus::testing::test_publisher()}}
            , name{"sem_reclaim."}
            , block(plexus::testing::xproc_shared<sem_reclaim_block>::create(name.str()))
            , io()
            , broker()
            , registry{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, posix_sem_leg::binder(io)}
            , doorbells{plexus::testing::region_name(fqn, plexus::testing::test_publisher())}
            , waiter()
    {
    }

    bool start()
    {
        if(!block || registry.acquire(fqn, k_request, plexus::testing::test_publisher(), plexus::shm::ring_role::producer, 64) != plexus::shm::acquire_result::created)
            return false;
        waiter                         = plexus::testing::xproc_process::spawn("shm_harness.sem_reclaim_waiter", {name.str(), fqn});
        const sem_reclaim_block &shown = block->get();
        wait_until([&] { return shown.ready.load(std::memory_order_acquire) != 0 || !waiter || !waiter->running(); }, 10s);
        return shown.ready.load(std::memory_order_acquire) != 0;
    }

    std::string doorbell()
    {
        const sem_reclaim_block &shown = block->get();
        return sem_name_for(plexus::testing::region_name(fqn, plexus::testing::test_publisher()), shown.slot.load(), shown.tag.load());
    }

    // The sends that completed before the first that did not.
    std::uint32_t send_until_refused()
    {
        auto *channel      = registry.channel_for(fqn, k_request, plexus::testing::test_publisher());
        std::uint32_t sent = 0;
        while(channel != nullptr && sent < k_sends && channel->send(std::as_bytes(std::span<const std::uint32_t, 1>(&sent, 1))) == plexus::shm::loan_status::ok)
            ++sent;
        return sent;
    }

    std::string fqn;
    topic_regions regions;
    plexus::testing::scoped_shm_name name;
    std::optional<plexus::testing::xproc_shared<sem_reclaim_block>> block;
    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    sem_registry registry;
    // Destroyed while the ring is still mapped, so it reads the tags a red run leaves named; macOS keeps such a name
    // until reboot.
    region_doorbells doorbells;
    std::optional<plexus::testing::xproc_process> waiter;
};

}

TEST_CASE("sem_reclaim a crashed waiter's named semaphore is unlinked when its cursor is reclaimed", "[shm][sem_reclaim]")
{
    reclaim_fixture fixture{"crashed"};
    REQUIRE(fixture.start());
    const std::string doorbell = fixture.doorbell();
    REQUIRE(open_error(doorbell) == 0);
    fixture.waiter->kill();

    CHECK(fixture.send_until_refused() == k_sends);
    CHECK(open_error(doorbell) == ENOENT);
}

TEST_CASE("sem_reclaim a live waiter's semaphore survives a reclaim pass", "[shm][sem_reclaim]")
{
    reclaim_fixture fixture{"live"};
    REQUIRE(fixture.start());
    const std::string doorbell = fixture.doorbell();
    REQUIRE(open_error(doorbell) == 0);

    CHECK(fixture.send_until_refused() < k_sends);
    CHECK(open_error(doorbell) == 0);
    fixture.block->get().leave.store(1, std::memory_order_release);
    CHECK(fixture.waiter->wait_for(10s) == std::optional<std::int32_t>(0));
}
