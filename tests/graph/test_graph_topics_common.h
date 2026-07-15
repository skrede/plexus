#ifndef HPP_GUARD_PLEXUS_TESTS_GRAPH_TOPICS_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_GRAPH_TOPICS_COMMON_H

#include "test_graph_node_common.h"

#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/topic_qos.h"
#include "plexus/subscriber.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"

#include "plexus/graph/topic_record.h"

#include <span>
#include <string>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>
#include <system_error>

#include <catch2/catch_test_macros.hpp>

namespace graph_topics_fixture {

using graph_node_fixture::host;
using graph_node_fixture::make_id;
using graph_node_fixture::inproc_node;
using plexus::graph::topic_role;
using plexus::graph::topic_record;

constexpr const char *k_imu_type_name      = "sensor/Imu";
constexpr std::string_view k_imu_topic     = "sensor/imu";
constexpr std::string_view k_plain_topic   = "plain/topic";
constexpr std::string_view k_unnamed_topic = "unnamed/topic";
constexpr std::uint64_t k_imu_type_id      = 0x1A2B3C4Du;
constexpr std::uint64_t k_unnamed_type_id  = 0x99u;

struct imu_codec
{
    using value_type = std::uint32_t;

    plexus::wire_bytes<> encode(const std::uint32_t &v) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, std::uint32_t &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        out = 0;
        for(int i = 0; i < 4; ++i)
            out |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {k_imu_type_id, k_imu_type_name};
    }
};

using imu_publisher  = plexus::publisher<imu_codec>;
using imu_subscriber = plexus::subscriber<imu_codec>;

// The sweep hands out views into the table's own storage, valid for that sweep only — an
// assertion outliving it needs its own copy.
struct edge_view
{
    plexus::node_id node;
    std::string topic;
    std::vector<std::string> types;
    topic_role role;
};

inline std::vector<edge_view> edges_of(const inproc_node &node)
{
    std::vector<edge_view> out;
    node.router().topic_table().for_each(
            [&](const topic_record &rec)
            {
                edge_view e{rec.node, std::string{rec.name}, {}, rec.role};
                for(std::size_t i = 0; i < rec.types.count; ++i)
                    e.types.emplace_back(rec.types.names[i]);
                out.push_back(std::move(e));
            });
    return out;
}

inline const edge_view *find_edge(const std::vector<edge_view> &edges, std::string_view topic, topic_role role)
{
    const auto it = std::find_if(edges.begin(), edges.end(), [&](const edge_view &e) { return e.topic == topic && e.role == role; });
    return it == edges.end() ? nullptr : &*it;
}

// Coherence with the participant enumeration: an edge may only name a peer that sweep lists.
inline bool holds_id(const inproc_node &node, const plexus::node_id &id)
{
    bool found = false;
    node.router().known().for_each([&](const plexus::node_id &known, const plexus::io::endpoint &) { found = found || known == id; });
    return found;
}

inline plexus::node_options opts_for(bool eager)
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
    opts.redial_seed  = 0x70B1C5u;
    opts.dial_eagerly = eager;
    return opts;
}

struct net
{
    host h;
    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};
    plexus::inproc::inproc_transport<> tb{h.ex, h.bus};

    inproc_node a{h.ex, h.disc, id_a, h.transport, opts_for(true)};
    inproc_node b{h.ex, h.disc, id_b, tb, opts_for(false)};

    void drive()
    {
        h.ex.drain();
    }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }

    // A real drop of the dialed session, driven from A's side; B's accepted end follows through
    // the channel close, so both peers observe the teardown the transport would deliver.
    void tear_down()
    {
        a.router().session_for(id_b)->tear_down();
        drive();
    }

    void reconnect()
    {
        a.router().reach(id_b);
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }
};

}

#endif
