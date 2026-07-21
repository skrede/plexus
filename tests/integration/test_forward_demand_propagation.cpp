// Demand propagation on the constrained leg: a downstream consumer subscribing a topic through a relay
// drives the relay to subscribe upstream on the origin's session ONLY for that topic, so the narrow
// origin<->relay leg carries data for demanded topics and NOTHING for undemanded ones. An optional
// forward-scope pattern narrows it further: an out-of-scope topic is never propagated upstream, so it
// never transits the narrow leg even when a downstream consumer asks for it. Driven over inproc
// routing engines (a relay-profile engine dialing a plain origin), with the downstream demand folded
// through the same attach_for_fanout the on-wire subscribe receive runs.

#include "test_routing_engine_inproc_common.h"

#include "plexus/io/detail/forward_splice.h"

#include "plexus/graph/topic_record.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <string_view>

using namespace routing_inproc_fixture;
namespace graph = plexus::graph;

namespace {

using relay_engine = plexus::io::routing_engine<manual_policy, transport_t, manual_clock, plexus::io::std_map_peer_storage, plexus::graph::std_map_topic_storage,
                                                plexus::io::default_liveliness_storage, plexus::graph::vector_graph_change_log, plexus::io::null_peer_report_emitter,
                                                plexus::io::forward_splice<manual_policy>>;

using consumer_channel = manual_policy::byte_channel_type;
using relay_forwarder  = plexus::io::message_forwarder<manual_policy>;

// A relay dialing a plain origin over one inproc bus. The origin declares its topics; the relay folds
// them into its topic table (the directly-attached-origin knowledge the demand-propagation seam reads).
struct relay_link
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t transport_relay{ex, bus};
    transport_t transport_origin{ex, bus};
    plexus::log::null_logger sink;

    relay_engine relay;
    engine origin;

    plexus::node_id relay_id{make_id(0x0A)};
    plexus::node_id origin_id{make_id(0x0B)};
    endpoint ep_origin{"inproc", "origin-node"};

    std::vector<std::pair<std::string, std::string>> relay_received;

    explicit relay_link(plexus::io::forward_options fwd = {})
            : relay(transport_relay, ex, make_cfg(0x0A), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, {}, {}, {}, fwd)
            , origin(transport_origin, ex, make_cfg(0x0B), k_long_timeout, forever_cfg(), k_seed, sink)
    {
        relay.on_message_route([this](std::string_view fqn, std::span<const std::byte> bytes, const plexus::io::message_info &)
                               { relay_received.emplace_back(std::string{fqn}, to_string(bytes)); });
    }

    void drive()
    {
        ex.drain();
    }

    // Origin declares a publisher topic and the relay dials + folds it before any demand is expressed.
    void connect_with_topics(std::span<const std::string_view> topics)
    {
        for(std::string_view t : topics)
            origin.messages().declare(t, plexus::topic_qos{});
        origin.listen(ep_origin);
        relay.note_peer(origin_id, ep_origin);
        relay.reach(origin_id);
        drive();
    }

    bool relay_knows_publisher(std::string_view fqn) const
    {
        bool found = false;
        relay.topic_table().for_each([&](const graph::topic_record &r)
                                     { found = found || (r.node == origin_id && r.role == graph::topic_role::publisher && r.name == fqn); });
        return found;
    }

    // The downstream demand: exactly what on_subscribe_received folds when a consumer's subscribe
    // arrives on the relay's downstream session.
    void downstream_subscribe(consumer_channel &consumer, std::string_view fqn)
    {
        relay.messages().attach_for_fanout(relay_forwarder::peer{consumer, "consumer"}, fqn);
        drive();
    }

    void origin_publish(std::string_view fqn, std::string_view body)
    {
        origin.publish(fqn, as_bytes(std::string{body}));
        drive();
    }

    bool relay_got(std::string_view fqn) const
    {
        for(const auto &[topic, body] : relay_received)
            if(topic == fqn)
                return true;
        return false;
    }
};

}

TEST_CASE("demand propagation: a downstream subscribe drives an upstream subscribe only for the demanded topic; "
          "an undemanded topic never crosses the narrow leg",
          "[integration][forward][demand]")
{
    manual_clock::reset();
    relay_link link;
    const std::array<std::string_view, 2> topics{"topic/a", "topic/b"};
    link.connect_with_topics(topics);
    REQUIRE(link.relay.is_connected(link.origin_id));
    REQUIRE(link.relay_knows_publisher("topic/a"));
    REQUIRE(link.relay_knows_publisher("topic/b"));

    // A consumer subscribes ONLY topic/a through the relay.
    consumer_channel consumer{link.ex};
    link.downstream_subscribe(consumer, "topic/a");

    // The origin now publishes BOTH topics. Only the demanded one has an upstream subscriber (the
    // relay), so only it crosses the narrow origin<->relay leg.
    link.origin_publish("topic/a", "hello-a");
    link.origin_publish("topic/b", "hello-b");

    REQUIRE(link.relay_got("topic/a"));        // demand propagated: the relay subscribed topic/a upstream
    REQUIRE_FALSE(link.relay_got("topic/b"));  // never demanded: no upstream subscribe, nothing transits
    REQUIRE(link.relay_received.size() == 1u); // exactly the one demanded topic crossed
    REQUIRE(link.relay_received.front().second == "hello-a");
}

TEST_CASE("demand propagation: a forward-scope pattern refuses an out-of-scope topic entirely — it never "
          "propagates upstream even when a downstream consumer demands it",
          "[integration][forward][demand][scope]")
{
    manual_clock::reset();
    plexus::io::forward_options scoped;
    scoped.scope_pattern = "sensor/*"; // admits sensor/temp, excludes cmd/reset
    relay_link link{scoped};
    const std::array<std::string_view, 2> topics{"sensor/temp", "cmd/reset"};
    link.connect_with_topics(topics);
    REQUIRE(link.relay.is_connected(link.origin_id));
    REQUIRE(link.relay_knows_publisher("sensor/temp"));
    REQUIRE(link.relay_knows_publisher("cmd/reset"));

    // A consumer subscribes BOTH — but the relay's forward scope admits only sensor/*.
    consumer_channel consumer_in{link.ex};
    consumer_channel consumer_out{link.ex};
    link.downstream_subscribe(consumer_in, "sensor/temp");
    link.downstream_subscribe(consumer_out, "cmd/reset");

    link.origin_publish("sensor/temp", "in-scope");
    link.origin_publish("cmd/reset", "out-of-scope");

    REQUIRE(link.relay_got("sensor/temp"));       // in-scope demand propagated upstream
    REQUIRE_FALSE(link.relay_got("cmd/reset"));   // out of scope: refused, never propagated, never transits
    REQUIRE(link.relay_received.size() == 1u);
}
