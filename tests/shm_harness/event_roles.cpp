#include "support/region_names.h"
#include "support/xproc_harness.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/shm/windows/event_notifier.h"

#include "plexus/native/win_shm_region_broker.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/same_host_geometry.h"

#include "plexus/muxify.h"

#include <asio/io_context.hpp>

#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <string_view>

namespace {

using namespace std::chrono_literals;

using event_policy   = plexus::muxify<plexus::asio::asio_policy>;
using event_notifier = plexus::asio::shm::event_notifier<event_policy>;
using event_registry = plexus::shm::shm_topic_registry<plexus::native::win_shm_region_broker, event_notifier>;

constexpr std::uint32_t k_arm_cycles = 100;

constexpr plexus::shm::ring_direction k_request = plexus::shm::ring_direction::request;

event_registry::notifier_binder event_binder(::asio::io_context &io)
{
    return [&io](std::optional<event_notifier> &slot, plexus::shm::control_header_t &header, std::string_view region) { slot.emplace(io, header, region); };
}

void settle(::asio::io_context &io, std::chrono::milliseconds span)
{
    io.restart();
    io.run_for(span);
}

// The first cycle settles so this process's waiter parks while the peer's waiter is parked on the same ring.
bool event_cycler(const std::vector<std::string> &argv)
{
    if(argv.size() != 1)
        return false;

    ::asio::io_context io;
    plexus::native::win_shm_region_broker broker;
    event_registry registry{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, event_binder(io)};
    for(std::uint32_t cycle = 0; cycle < k_arm_cycles; ++cycle)
    {
        if(registry.acquire(argv[0], k_request, plexus::testing::test_publisher(), 0, plexus::io::ring_geometry_mode::reliable_preserving, 0, plexus::shm::acquire_mode::join_live) ==
           plexus::shm::acquire_result::failed)
            return false;
        settle(io, cycle == 0 ? 200ms : 1ms);
        registry.release(argv[0], k_request, plexus::testing::test_publisher());
    }
    return true;
}

[[maybe_unused]] const bool s_cycler_registered = plexus::testing::register_xproc_child("shm_harness.event_cycler", &event_cycler);

}
