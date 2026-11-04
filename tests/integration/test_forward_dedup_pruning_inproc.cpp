#include "test_forward_pubsub_common.h"

#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/liveliness_options.h"

#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

using namespace forward_pubsub_fixture;

namespace {

constexpr std::string_view k_topic = "pruning/temp";
constexpr auto k_ttl               = std::chrono::seconds(2);
constexpr auto k_aged              = k_ttl + 2 * plexus::io::k_tick_granularity;

plexus::io::liveliness_options short_awareness()
{
    plexus::io::liveliness_options live{};
    live.awareness_ttl = k_ttl;
    return live;
}

template<typename Engine>
struct incarnation
{
    incarnation(inproc_executor<manual_clock> &ex, inproc_bus<manual_clock> &bus, std::uint8_t seed)
            : transport(ex, bus)
            , log()
            , e(transport, ex, make_cfg(seed), k_long_timeout, forever_cfg(), k_seed, log)
    {
    }

    transport_t transport;
    plexus::log::null_logger log;
    Engine e;
};

struct dedup_line
{
    dedup_line()
            : bus()
            , ex(bus)
            , t_sub(ex, bus)
            , sink()
            , sub(t_sub, ex, make_cfg(0x5B), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, short_awareness())
            , relay()
            , pub()
            , delivered(0)
            , id_pub(make_id(0xB0))
            , id_relay(make_id(0x2A))
            , ep_relay("inproc", "pruning-relay")
    {
        sub.on_message_route([this](std::string_view, std::span<const std::byte>, const plexus::io::message_info &) { ++delivered; });
        start_relay();
        start_pub();
        sub.note_peer(id_relay, ep_relay);
        sub.subscribe(id_relay, k_topic);
        ex.drain();
    }

    void start_relay()
    {
        relay.emplace(ex, bus, 0x2A);
        relay->e.listen(ep_relay);
    }

    void start_pub()
    {
        pub.emplace(ex, bus, 0xB0);
        pub->e.messages().declare(k_topic, plexus::topic_qos{});
        pub->e.note_peer(id_relay, ep_relay);
        pub->e.reach(id_relay);
        ex.drain();
    }

    template<typename Engine>
    void stop(std::optional<incarnation<Engine>> &node)
    {
        node->e.close();
        ex.drain();
        node.reset();
        ex.drain();
    }

    void publish(std::size_t n)
    {
        for(std::size_t i = 0; i < n; ++i)
        {
            pub->e.publish(k_topic, as_bytes(std::string{"sample"}));
            ex.drain();
        }
    }

    std::size_t windows()
    {
        return sub.messages().forward_dedup().tracked();
    }

    void run_for(std::chrono::nanoseconds d)
    {
        for(std::chrono::nanoseconds t{0}; t < d; t += plexus::io::k_tick_granularity)
        {
            manual_clock::advance(plexus::io::k_tick_granularity);
            ex.drain();
        }
    }

    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex;
    transport_t t_sub;
    plexus::log::null_logger sink;
    engine sub;
    std::optional<incarnation<relay_engine>> relay;
    std::optional<incarnation<engine>> pub;
    std::size_t delivered;
    node_id id_pub;
    node_id id_relay;
    endpoint ep_relay;
};

}

TEST_CASE("forward dedup: an origin that ages out and returns through the same relay is delivered again", "[integration][forward_dedup]")
{
    manual_clock::reset();
    dedup_line line;
    line.sub.note_peer(line.id_pub, endpoint{"inproc", "pruning-pub"});
    line.publish(40);
    REQUIRE(line.delivered == 40);

    line.stop(line.pub);
    line.run_for(k_aged);
    REQUIRE_FALSE(line.sub.known().contains(line.id_pub));

    line.start_pub();
    line.publish(1);
    CHECK(line.delivered == 41);
}

TEST_CASE("forward dedup: a relay that restarts with a fresh sequence is delivered again", "[integration][forward_dedup]")
{
    manual_clock::reset();
    dedup_line line;
    line.publish(40);
    REQUIRE(line.delivered == 40);

    line.stop(line.relay);
    line.start_relay();
    line.run_for(std::chrono::seconds(1));
    REQUIRE(line.sub.is_connected(line.id_relay));
    REQUIRE(line.pub->e.is_connected(line.id_relay));

    line.publish(1);
    CHECK(line.delivered == 41);
}

TEST_CASE("forward dedup: a relay's teardown drops the windows that arrived through it", "[integration][forward_dedup]")
{
    manual_clock::reset();
    dedup_line line;
    line.publish(1);
    REQUIRE(line.windows() == 1);

    line.stop(line.relay);
    line.run_for(std::chrono::seconds(1));
    REQUIRE_FALSE(line.sub.is_connected(line.id_relay));
    CHECK(line.windows() == 0);
}

TEST_CASE("forward dedup: an origin that ages out leaves no window behind", "[integration][forward_dedup]")
{
    manual_clock::reset();
    dedup_line line;
    line.sub.note_peer(line.id_pub, endpoint{"inproc", "pruning-pub"});
    line.publish(1);
    REQUIRE(line.windows() == 1);

    SECTION("by aging out")
    {
        line.run_for(k_aged);
        REQUIRE_FALSE(line.sub.known().contains(line.id_pub));
    }
    SECTION("by a forget")
    {
        line.sub.forget(line.id_pub);
    }
    REQUIRE(line.sub.is_connected(line.id_relay));
    CHECK(line.windows() == 0);
}
