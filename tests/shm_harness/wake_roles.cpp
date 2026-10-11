#include "child_args.h"
#include "wake_block.h"
#include "wake_producer.h"

#include "support/xproc_control.h"
#include "support/xproc_harness.h"

#include "plexus/asio/shm/shm_member.h"
#include "plexus/asio/shm/shm_notifier.h"
#include "plexus/asio/shm/linux/futex_notifier.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/ring_geometry_mode.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/wire_bytes.h"

#include <asio/io_context.hpp>

#include <chrono>
#include <random>
#include <string>
#include <vector>
#include <csignal>
#include <cstdint>
#include <optional>
#include <string_view>
#include <type_traits>

#include <sys/prctl.h>

namespace {

using namespace shm_harness;
using namespace std::chrono_literals;

using threaded_notifier = plexus::asio::shm::ring_notifier_threaded<wake_policy>;

template<typename Notifier>
using registry_for = plexus::shm::shm_topic_registry<plexus::native::posix_shm_region_broker, Notifier>;

constexpr plexus::shm::ring_direction k_request = plexus::shm::ring_direction::request;

template<typename Notifier>
typename registry_for<Notifier>::notifier_binder binder_on(::asio::io_context &io)
{
    if constexpr(std::is_same_v<Notifier, production_notifier>)
        return plexus::asio::shm::make_bridge_binder(io);
    else
        return [&io](std::optional<Notifier> &slot, plexus::shm::control_header_t &header, std::string_view region) { slot.emplace(io, header, region); };
}

template<typename Notifier>
bool serve_consumer(const std::string &fqn, wake_block &block, wake_consumer_slot &slot)
{
    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    registry_for<Notifier> registry{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, binder_on<Notifier>(io)};
    const plexus::shm::acquire_result acquired = registry.acquire(fqn, k_request, 0, plexus::shm::ring_geometry_mode::reliable_preserving, 0, plexus::shm::acquire_mode::join_live);
    if(acquired == plexus::shm::acquire_result::failed)
        return false;

    registry.set_consumer_sink(fqn, k_request, [&slot](plexus::wire_bytes<plexus::shm::shm_slot_owner>) { slot.received.fetch_add(1, std::memory_order_acq_rel); });
    slot.ready.store(1, std::memory_order_release);
    while(block.leave.load(std::memory_order_acquire) == 0)
    {
        io.restart();
        io.run_for(1ms);
    }
    registry.release(fqn, k_request);
    return true;
}

bool wake_consumer(const std::vector<std::string> &argv)
{
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
    const std::optional<std::uint32_t> index = argv.size() == 4 ? to_u32(argv[1]) : std::nullopt;
    auto block                               = plexus::testing::xproc_shared<wake_block>::attach(argv.empty() ? std::string() : argv[0]);
    if(!index || *index >= k_wake_consumers || !block)
        return false;

    wake_consumer_slot &slot = block->get().consumers[*index];
    if(argv[3] == k_threaded_leg)
        return serve_consumer<threaded_notifier>(argv[2], block->get(), slot);
    return serve_consumer<production_notifier>(argv[2], block->get(), slot);
}

bool attach_ring(plexus::native::posix_shm_region_broker &broker, const std::string &name, attached_ring &out)
{
    return broker.attach(name, out.control) == plexus::shm::region_status::ok && broker.attach(name + ".s", out.slab) == plexus::shm::region_status::ok &&
            plexus::shm::broadcast_ring::attach(out.control.bytes(), out.slab.bytes(), out.ring) == plexus::shm::loan_status::ok;
}

bool run_trials(burst_producer &producer, std::uint32_t trials)
{
    std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<std::uint32_t> burst_size(1, 200);
    for(std::uint32_t trial = 0; trial < trials; ++trial)
    {
        if(!producer.burst(burst_size(rng)) || !producer.observe())
            return false;
    }
    producer.report(trials);
    return true;
}

bool wake_producer(const std::vector<std::string> &argv)
{
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
    const std::optional<std::uint32_t> consumers = argv.size() >= 3 ? to_u32(argv[2]) : std::nullopt;
    const std::optional<std::uint32_t> trials    = argv.size() == 4 ? to_u32(argv[3]) : std::optional<std::uint32_t>(500);
    auto block                                   = plexus::testing::xproc_shared<wake_block>::attach(argv.empty() ? std::string() : argv[0]);
    if(!consumers || *consumers == 0 || *consumers > k_wake_consumers || !trials || !block)
        return false;

    plexus::native::posix_shm_region_broker broker;
    const std::string region = plexus::shm::region_name_for(argv[1], k_request);
    attached_ring attached;
    if(!attach_ring(broker, region, attached))
        return false;

    ::asio::io_context io;
    production_notifier notifier{io, attached.ring.header(), region};
    burst_producer producer{attached.ring, notifier, block->get(), *consumers};
    return run_trials(producer, *trials);
}

[[maybe_unused]] const bool s_consumer_registered = plexus::testing::register_xproc_child("shm_harness.wake_consumer", &wake_consumer);
[[maybe_unused]] const bool s_producer_registered = plexus::testing::register_xproc_child("shm_harness.wake_producer", &wake_producer);

}
