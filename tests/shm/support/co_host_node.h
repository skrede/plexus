#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_CO_HOST_NODE_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_CO_HOST_NODE_H

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/stream_socket_options.h"

#include "plexus/asio/shm/shm_member.h"

#include "plexus/shm/region_naming.h"

#include "plexus/native/shm_region_broker.h"
#include "plexus/native/machine_fingerprint.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/dispatch_hint.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/egress_capacity.h"

#include "plexus/node.h"
#include "plexus/node_id.h"
#include "plexus/topic_qos.h"
#include "plexus/node_options.h"

#include "plexus/testing/platform.h"

#include <asio/io_context.hpp>

#include <span>
#include <array>
#include <atomic>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <algorithm>

namespace plexus::testing {

using co_host_node_type = plexus::node<plexus::asio::asio_policy, plexus::asio::shm::shm_member, plexus::asio::unix_transport, plexus::asio::asio_transport>;

// Every node of one case shares this namespace, so they meet on each other's rings while other cases' rings stay apart.
inline std::string co_host_region(const std::string &prefix)
{
    static std::atomic<std::uint32_t> next{0};
    return prefix + std::to_string(process_id()) + "-" + std::to_string(next.fetch_add(1));
}

inline plexus::node_options co_host_options(const std::string &name)
{
    plexus::node_options opts;
    opts.name                        = name;
    opts.handshake.local_fingerprint = plexus::native::read_machine_fingerprint();
    return opts;
}

inline plexus::node_id co_host_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// One same-host node on its own io_context, so a case can run one node while another is left unread.
struct co_host_node
{
    co_host_node(const std::string &name, std::uint8_t seed, const std::string &region_ns, plexus::asio::stream_socket_options sockets = {})
            : io()
            , broker()
            , disc(std::vector<plexus::discovery::service_info>{})
            , shm(plexus::asio::shm::make_shm_member(io, broker, plexus::io::reliability::reliable, plexus::io::congestion::block, region_ns))
            , local(io)
            , remote(io, {}, true, plexus::io::congestion::block, plexus::io::egress_capacity::bounded_default(), sockets)
            , node(io, disc, co_host_id(seed), shm, local, remote, co_host_options(name))
    {
    }

    // The subscriber half of this node attached to the publisher's ring for fqn.
    bool reads_ring_of(const co_host_node &publisher, const std::string &fqn)
    {
        return shm.registry().channel_for(fqn, plexus::shm::ring_direction::request, publisher.node.id()) != nullptr;
    }

    void run_for(std::chrono::milliseconds budget)
    {
        io.restart();
        io.run_for(budget);
    }

    ::asio::io_context io;
    plexus::native::shm_region_broker broker;
    plexus::discovery::static_discovery disc;
    plexus::asio::shm::shm_member shm;
    plexus::asio::unix_transport local;
    plexus::asio::asio_transport remote;
    co_host_node_type node;
};

// Each node runs one turn at a time, so a case decides which node reads what and when.
template<typename Done>
bool step_both(co_host_node &a, co_host_node &b, Done done, std::chrono::milliseconds budget)
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(!done())
    {
        if(std::chrono::steady_clock::now() >= deadline)
            return false;
        a.run_for(std::chrono::milliseconds{1});
        b.run_for(std::chrono::milliseconds{1});
    }
    return true;
}

inline void settle(co_host_node &a, co_host_node &b)
{
    step_both(a, b, [] { return false; }, std::chrono::milliseconds{300});
}

inline plexus::topic_qos hinted_topic()
{
    plexus::topic_qos qos;
    qos.dispatch = plexus::io::dispatch_hint::frequent;
    return qos;
}

inline plexus::io::subscriber_qos hinted_subscriber()
{
    plexus::io::subscriber_qos qos;
    qos.dispatch = plexus::io::dispatch_hint::frequent;
    return qos;
}

inline std::array<std::byte, 4> index_bytes(std::uint32_t index)
{
    std::array<std::byte, 4> bytes{};
    std::memcpy(bytes.data(), &index, sizeof(index));
    return bytes;
}

inline std::uint32_t index_of(std::span<const std::byte> bytes)
{
    std::uint32_t index = 0;
    std::memcpy(&index, bytes.data(), std::min(bytes.size(), sizeof(index)));
    return index;
}

}

#endif
