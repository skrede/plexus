#include "child_args.h"
#include "node_config.h"
#include "node_driver.h"
#include "seccomp_deny.h"
#include "notifier_legs.h"
#include "topology_block.h"

#include "support/xproc_control.h"
#include "support/xproc_harness.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/same_host_transports.h"

#include "plexus/shm/shm_mux_member.h"
#include "plexus/shm/fallback_notifier.h"

#include "plexus/native/shm_region_broker.h"
#include "plexus/native/machine_fingerprint.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <asio/io_context.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

#include <sys/prctl.h>

namespace {

using namespace shm_harness;
using namespace std::chrono_literals;

using denied_member = plexus::shm::shm_mux_member<plexus::native::shm_region_broker, plexus::shm::fallback_notifier<plexus::testing::denied_primary, plexus::testing::denied_floor>>;
using lost_notifier = plexus::shm::fallback_notifier<plexus::testing::demoting_primary<plexus::asio::asio_policy>, plexus::testing::denied_floor>;
using lost_member   = plexus::shm::shm_mux_member<plexus::native::shm_region_broker, lost_notifier>;

template<typename Node>
void update_connected(Node &node, const node_args &args, node_slot &slot)
{
    std::uint32_t connected = 0;
    for(std::uint32_t peer = 0; peer < args.count; ++peer)
    {
        if(dials(args, peer) && node.router().is_connected(harness_id(peer)))
            ++connected;
    }
    slot.connected.store(connected, std::memory_order_release);
}

template<typename Node>
void serve_node(::asio::io_context &io, Node &node, const node_args &args, node_slot &slot)
{
    node.listen({"tcp", "127.0.0.1:" + std::to_string(args.ports[args.index])});

    std::optional<plexus::publisher<>> pub;
    if(args.role != role_name(node_role::sub))
        pub.emplace(node, args.fqn, topic_qos_for(args.hint));
    node_driver driver{slot, args.index, pub ? publish_fn{[&pub](std::span<const std::byte> bytes) { pub->publish(bytes); }} : publish_fn{}};
    std::optional<plexus::subscriber<>> sub;
    if(args.role != role_name(node_role::pub))
        sub.emplace(node, args.fqn, subscriber_qos_for(args.hint), [&driver](std::span<const std::byte> bytes) { driver.receive(bytes); });

    slot.ready.store(1, std::memory_order_release);
    while(driver.serve())
    {
        io.restart();
        io.run_for(1ms);
        update_connected(node, args, slot);
    }
}

void run_same_host(const node_args &args, node_slot &slot)
{
    ::asio::io_context io;
    plexus::discovery::static_discovery disc{peer_cards(args)};
    plexus::asio::same_host_transports transports{io};
    auto node = transports.make_node(disc, harness_id(args.index), dial_opts());
    serve_node(io, node, args, slot);
}

// The production leaves with a test shared-memory member; the fingerprint is the real one, so the node attempts the
// same-host upgrade exactly as the production composition does.
template<typename Member, typename Bind>
void run_test_member(const node_args &args, node_slot &slot, Bind bind)
{
    ::asio::io_context io;
    plexus::discovery::static_discovery disc{peer_cards(args)};
    plexus::native::shm_region_broker broker;
    Member shm{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, bind(io)};
    plexus::asio::unix_transport local{io};
    plexus::asio::asio_transport remote{io};
    plexus::node_options opts        = dial_opts();
    opts.handshake.local_fingerprint = plexus::native::read_machine_fingerprint();
    plexus::node<plexus::asio::asio_policy, Member, plexus::asio::unix_transport, plexus::asio::asio_transport> node{io, disc, harness_id(args.index), shm, local, remote, opts};
    serve_node(io, node, args, slot);
}

void run_node(const node_args &args, node_slot &slot)
{
    if(args.composition == composition_name(node_composition::arm_denied))
        return run_test_member<denied_member>(args, slot, [](::asio::io_context &) { return denied_member::registry_type::default_notifier_binder(); });
    if(args.composition == composition_name(node_composition::runtime_lost))
        return run_test_member<lost_member>(args, slot, [](::asio::io_context &io) -> lost_member::notifier_binder
                                            { return [&io](std::optional<lost_notifier> &n, plexus::shm::control_header_t &h, std::string_view name) { n.emplace(io, h, name); }; });
    run_same_host(args, slot);
}

// The parent-death signal keeps a child from outliving a parent that was killed before it could reap it.
bool node_child(const std::vector<std::string> &argv)
{
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
    const std::optional<node_args> args = parse_node_args(argv);
    if(!args || (args->deny && !deny_io_uring()))
        return false;
    auto block = plexus::testing::xproc_shared<topology_block>::attach(args->block);
    if(!block)
        return false;

    node_slot &slot = block->get().nodes[args->index];
    run_node(*args, slot);
    slot.exit_code.store(0, std::memory_order_release);
    return true;
}

[[maybe_unused]] const bool s_node_registered = plexus::testing::register_xproc_child("shm_harness.node", &node_child);

}
