#include "test_node_close_quiescence_common.h"

#include "plexus/node.h"
#include "plexus/target_profile.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/endpoint_id.h"

#include "plexus/graph/participant_record.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <utility>
#include <optional>
#include <algorithm>

using namespace node_close_fixture;

namespace {

using relay_node = plexus::node<plexus::relay<inproc_policy>, inproc_transport<>>;

std::optional<plexus::graph::participant_record> record_of(const listener_node &node, const plexus::node_id &id)
{
    std::array<plexus::graph::participant_record, 8> buf{};
    const auto res = node.participants(buf);
    const std::span<const plexus::graph::participant_record> found{buf.data(), res.count};
    const auto it = std::find_if(found.begin(), found.end(), [&](const auto &r) { return r.id == id; });
    if(it == found.end())
        return std::nullopt;
    return *it;
}

// The relay dials both; its slot for the origin sorts before its consumer's, so the origin's session is torn down while
// the consumer's is still complete.
struct relay_net
{
    inproc_bus<> bus;
    inproc_executor<> ex;
    inproc_transport<> to;
    inproc_transport<> tc;
    inproc_transport<> tr;
    counting_discovery disc_o;
    counting_discovery disc_c;
    counting_discovery disc_r;
    endpoint ep_o;
    endpoint ep_c;
    plexus::node_id origin_id;
    plexus::node_id relay_id;
    listener_node origin;
    listener_node consumer;
    relay_node relay;

    relay_net()
            : ex{bus}
            , to{ex, bus}
            , tc{ex, bus}
            , tr{ex, bus}
            , ep_o{"inproc", "host-o:5000"}
            , ep_c{"inproc", "host-c:6000"}
            , origin_id{make_id(0x0A)}
            , relay_id{make_id(0x0B)}
            , origin{ex, disc_o, origin_id, to, make_opts()}
            , consumer{ex, disc_c, make_id(0x0C), tc, make_opts()}
            , relay{ex, disc_r, relay_id, tr, make_opts()}
    {
        if(plexus::io::endpoint_id(ep_c) < plexus::io::endpoint_id(ep_o))
            std::swap(ep_o, ep_c);
        origin.listen(ep_o);
        consumer.listen(ep_c);
        relay.dial(ep_o);
        relay.dial(ep_c);
        ex.drain();
    }
};

}

TEST_CASE("node_close a closing relay withdraws no origin from its consumers", "[node][node_close]")
{
    relay_net net;
    const auto before = record_of(net.consumer, net.origin_id);
    REQUIRE(before.has_value());
    REQUIRE(before->reach.via == net.relay_id);
    REQUIRE(before->origin.reach_status == plexus::graph::reachability::reachable);

    close_or_fail(net.relay);
    net.ex.drain();
    const auto after = record_of(net.consumer, net.origin_id);
    REQUIRE(after.has_value());
    REQUIRE(after->origin.reach_status == plexus::graph::reachability::unreachable);
}
