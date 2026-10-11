#include "child_args.h"
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
#include "plexus/node_id.h"
#include "plexus/publisher.h"
#include "plexus/topic_qos.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/discovery/contact_card.h"
#include "plexus/discovery/static_discovery.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/reconnect_config.h"

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

plexus::node_id harness_id(std::uint32_t index)
{
    plexus::node_id id{};
    id[0] = std::byte(0x40 + index);
    return id;
}

// One-way wiring gives only the higher-indexed node of each pair the other's card, so exactly one side dials; crossed
// wiring gives every node every card, so both sides of each pair dial.
bool dials(const node_args &args, std::uint32_t peer)
{
    return peer != args.index && (args.crossed || peer < args.index);
}

std::vector<plexus::discovery::service_info> peer_cards(const node_args &args)
{
    std::vector<plexus::discovery::service_info> cards;
    for(std::uint32_t peer = 0; peer < args.count; ++peer)
    {
        if(!dials(args, peer))
            continue;
        const auto card = plexus::discovery::assemble_contact_card(harness_id(peer), {{"tcp", args.ports[peer]}});
        cards.push_back({"shm_harness." + std::to_string(peer), plexus::io::endpoint{"", "127.0.0.1"}, card});
    }
    return cards;
}

plexus::node_options dial_opts()
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{20ms, 500ms, std::nullopt, std::nullopt};
    opts.redial_seed  = 0xC0FFEEu;
    opts.dial_eagerly = true;
    return opts;
}

plexus::topic_qos topic_qos_for(bool hint)
{
    plexus::topic_qos qos;
    qos.dispatch = hint ? plexus::io::dispatch_hint::frequent : plexus::io::dispatch_hint::none;
    return qos;
}

plexus::io::subscriber_qos subscriber_qos_for(bool hint)
{
    plexus::io::subscriber_qos qos;
    qos.dispatch = hint ? plexus::io::dispatch_hint::frequent : plexus::io::dispatch_hint::none;
    return qos;
}

using denied_member = plexus::shm::shm_mux_member<plexus::native::shm_region_broker, plexus::shm::fallback_notifier<plexus::testing::denied_primary, plexus::testing::denied_floor>>;

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

// The production leaves with a shared-memory member whose notifiers all fail to arm; the fingerprint is the real one,
// so the node attempts the same-host upgrade exactly as the production composition does.
void run_arm_denied(const node_args &args, node_slot &slot)
{
    ::asio::io_context io;
    plexus::discovery::static_discovery disc{peer_cards(args)};
    plexus::native::shm_region_broker broker;
    denied_member shm{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};
    plexus::asio::unix_transport local{io};
    plexus::asio::asio_transport remote{io};
    plexus::node_options opts        = dial_opts();
    opts.handshake.local_fingerprint = plexus::native::read_machine_fingerprint();
    plexus::node<plexus::asio::asio_policy, denied_member, plexus::asio::unix_transport, plexus::asio::asio_transport> node{io, disc, harness_id(args.index), shm, local, remote, opts};
    serve_node(io, node, args, slot);
}

void run_node(const node_args &args, node_slot &slot)
{
    if(args.composition == composition_name(node_composition::arm_denied))
        run_arm_denied(args, slot);
    else
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
