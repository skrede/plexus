#include "child_args.h"
#include "node_driver.h"
#include "topology_block.h"

#include "support/xproc_control.h"
#include "support/xproc_harness.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/same_host_transports.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/shm/shm_channel.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_selection.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/ring_geometry_mode.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/fragmentation.h"

#include "plexus/muxify.h"
#include "plexus/wire_bytes.h"

#include <asio/io_context.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <optional>

#include <sys/prctl.h>

namespace {

using namespace shm_harness;
using namespace std::chrono_literals;

using registry_type = plexus::asio::shm::shm_member::registry_type;
using channel_type  = plexus::shm::shm_channel<plexus::asio::shm::ring_notifier<plexus::muxify<plexus::asio::asio_policy>>>;

constexpr plexus::shm::ring_direction k_request = plexus::shm::ring_direction::request;

// Each send is followed by a non-blocking turn of the reactor, so this process's own consumer cursor keeps draining
// through a burst and only a peer's cursor gates the ring.
publish_fn registry_publisher(::asio::io_context &io, channel_type &channel, node_slot &slot)
{
    return [&io, &channel, &slot](std::span<const std::byte> bytes)
    {
        const auto start = std::chrono::steady_clock::now();
        channel.send(bytes);
        const auto took = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
        if(static_cast<std::uint32_t>(took.count()) > slot.slowest_send_us.load(std::memory_order_relaxed))
            slot.slowest_send_us.store(static_cast<std::uint32_t>(took.count()), std::memory_order_release);
        io.restart();
        io.poll();
    };
}

// A publisher sizes the ring for the default message size, the geometry a default node publisher provisions; a
// subscriber passes 0 and adopts the geometry of the ring it joins.
channel_type *join_ring(registry_type &registry, const std::string &fqn, bool publishes)
{
    const std::size_t message_bytes = publishes ? plexus::io::global_default_max_message_bytes : 0;
    const std::uint32_t max_payload = plexus::shm::upgrade_ring_max_payload(k_request, static_cast<std::uint32_t>(message_bytes));
    const plexus::shm::acquire_result acquired =
            registry.acquire(fqn, k_request, max_payload, plexus::shm::ring_geometry_mode::reliable_preserving, 0, plexus::shm::acquire_mode::join_live);
    return acquired == plexus::shm::acquire_result::failed ? nullptr : registry.channel_for(fqn, k_request);
}

bool run_registry(const registry_args &args, node_slot &slot)
{
    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    registry_type registry{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, plexus::asio::shm::make_bridge_binder(io)};
    const bool publishes  = args.role == role_name(node_role::pub);
    channel_type *channel = join_ring(registry, args.fqn, publishes);
    if(channel == nullptr)
        return false;

    node_driver driver{slot, args.index, publishes ? registry_publisher(io, *channel, slot) : publish_fn{}};
    if(!publishes)
        registry.set_consumer_sink(args.fqn, k_request, [&driver](plexus::wire_bytes<plexus::shm::shm_slot_owner> wb) { driver.receive({wb.data(), wb.size()}); });

    slot.ready.store(1, std::memory_order_release);
    while(driver.serve())
    {
        io.restart();
        io.run_for(1ms);
    }
    registry.release(args.fqn, k_request);
    return true;
}

bool registry_child(const std::vector<std::string> &argv)
{
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
    const std::optional<registry_args> args = parse_registry_args(argv);
    if(!args)
        return false;
    auto block = plexus::testing::xproc_shared<topology_block>::attach(args->block);
    if(!block)
        return false;

    node_slot &slot = block->get().nodes[args->index];
    if(!run_registry(*args, slot))
        return false;
    slot.exit_code.store(0, std::memory_order_release);
    return true;
}

[[maybe_unused]] const bool s_registry_registered = plexus::testing::register_xproc_child("shm_harness.registry", &registry_child);

}
