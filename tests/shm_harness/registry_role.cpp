#include "child_args.h"
#include "node_config.h"
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
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/same_host_geometry.h"

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

publish_fn registry_publisher(channel_type &channel, node_slot &slot)
{
    return [&channel, &slot](std::span<const std::byte> bytes)
    {
        const auto start = std::chrono::steady_clock::now();
        channel.send(bytes);
        const auto took        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
        const std::uint32_t us = static_cast<std::uint32_t>(took.count());
        record_send(slot.sends, us);
        if(us > slot.slowest_send_us.load(std::memory_order_relaxed))
            slot.slowest_send_us.store(us, std::memory_order_release);
    };
}

plexus::shm::ring_role role_of(bool publishes)
{
    return publishes ? plexus::shm::ring_role::producer : plexus::shm::ring_role::consumer;
}

// A publisher sizes the ring for the default message size, the geometry a default node publisher provisions; a
// subscriber joins the ring as the publisher made it. Every role names the ring of node 0, the publisher.
channel_type *join_ring(registry_type &registry, const std::string &fqn, bool publishes)
{
    const std::uint32_t max_payload = plexus::shm::upgrade_ring_max_payload(k_request, static_cast<std::uint32_t>(plexus::io::global_default_max_message_bytes));
    const plexus::shm::acquire_result acquired =
            registry.acquire(fqn, k_request, harness_id(0), role_of(publishes), max_payload, plexus::io::ring_geometry_mode::reliable_preserving, 0);
    return acquired == plexus::shm::acquire_result::failed ? nullptr : registry.channel_for(fqn, k_request, harness_id(0));
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

    node_driver driver{slot, args.index, publishes ? registry_publisher(*channel, slot) : publish_fn{}};
    if(!publishes)
        registry.set_consumer_sink(args.fqn, k_request, harness_id(0), [&driver](plexus::wire_bytes<plexus::shm::shm_slot_owner> wb) { driver.receive({wb.data(), wb.size()}); });

    slot.ready.store(1, std::memory_order_release);
    while(driver.serve())
    {
        io.restart();
        io.run_for(1ms);
    }
    registry.release(args.fqn, k_request, harness_id(0), role_of(publishes));
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
