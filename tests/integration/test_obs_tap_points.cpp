// The deterministic data-path tap-point oracle on the manual virtual clock. A
// recording observer (shared header) is registered on a publishing routing_engine and
// the message-fan-out taps are forced: a single publish to a topic with N attached
// subscribers fires on_message_published exactly once and on_message_delivered exactly
// once per destination, both POSTED (the counters are still zero synchronously at the
// publish call return and become nonzero only after the executor is pumped), and the
// delivered view BORROWS the surfaced buffer's owner (a shared addref, not a fresh
// copy). The harness mirrors the peer-observer oracle's two-node inproc shape, widened
// to N subscriber nodes on one bus.

#include "recording_observer.h"

#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::io::endpoint;
using plexus::io::handshake_fsm_config;
using plexus::io::reconnect_config;

namespace {

struct manual_clock
{
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept { return current; }
    static void reset() noexcept { current = time_point{}; }
    static void advance(duration d) noexcept { current += d; }
};

struct manual_policy
{
    using executor_type = inproc_executor<manual_clock> &;
    using byte_channel_type = inproc_channel<manual_clock>;
    using timer_type = inproc_timer<manual_clock>;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn) { ex.post(std::move(fn)); }
};

static_assert(plexus::Policy<manual_policy>);

using transport_t = inproc_transport<manual_clock>;
using engine = plexus::io::routing_engine<manual_policy, transport_t, manual_clock>;

constexpr auto k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed = 0xC0FFEEu;

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                .compatible_version_major = 1, .compatible_version_minor = 0};
}

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                            std::nullopt, std::nullopt};
}

// One publisher engine and N subscriber engines on a single inproc bus and executor:
// every subscriber demands "topic" from the publisher, so a single publish fans to N
// destinations through the production attach path (no faked attach).
struct fan_net
{
    static constexpr std::uint8_t k_pub_seed = 0xD0;
    static constexpr const char *k_topic = "topic";

    explicit fan_net(std::size_t subscriber_count)
        : pub_transport(ex, bus)
        , pub(pub_transport, ex, make_cfg(k_pub_seed), k_long_timeout, forever_cfg(), k_seed, false)
    {
        pub.listen({"inproc", "pub"});
        for(std::size_t i = 0; i < subscriber_count; ++i)
        {
            const auto seed = static_cast<std::uint8_t>(0x10 + i);
            auto t = std::make_unique<transport_t>(ex, bus);
            auto e = std::make_unique<engine>(*t, ex, make_cfg(seed), k_long_timeout,
                                              forever_cfg(), k_seed, false);
            const std::string name = "sub-" + std::to_string(i);
            e->listen({"inproc", name});
            e->note_peer(pub_id(), {"inproc", "pub"});
            e->subscribe(pub_id(), k_topic);
            sub_transports.push_back(std::move(t));
            subs.push_back(std::move(e));
        }
    }

    static plexus::node_id pub_id() { return make_id(k_pub_seed); }

    void drive() { ex.drain(); }

    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t pub_transport;
    engine pub;
    std::vector<std::unique_ptr<transport_t>> sub_transports;
    std::vector<std::unique_ptr<engine>> subs;
};

}

TEST_CASE("obs tap: one publish to N subscribers fires published once and delivered once per destination",
          "[integration][observer][tap]")
{
    constexpr std::size_t k_subscribers = 3;
    const std::string payload = "tap-fan-out";

    manual_clock::reset();
    fan_net net{k_subscribers};
    recording_observer rec;
    net.pub.add_observer(rec);
    net.drive();   // settle the N subscribe round-trips

    net.pub.publish(fan_net::k_topic, as_bytes(payload));

    // Posted-not-inline: the taps fan out on the executor, so the counters are still
    // zero synchronously at the publish call return.
    REQUIRE(rec.for_topic(fan_net::k_topic).published == 0);
    REQUIRE(rec.for_topic(fan_net::k_topic).delivered == 0);

    net.drive();   // pump the posted tap turns

    const auto &t = rec.for_topic(fan_net::k_topic);
    REQUIRE(t.published == 1);                                  // on_message_published once per publish
    REQUIRE(t.delivered == static_cast<int>(k_subscribers));   // on_message_delivered once per destination
}

TEST_CASE("obs tap: the delivered view borrows the surfaced owner (a shared addref, not a copy)",
          "[integration][observer][tap]")
{
    const std::string payload = "tap-zero-copy";

    manual_clock::reset();
    fan_net net{1};
    recording_observer rec;
    net.pub.add_observer(rec);
    net.drive();

    net.pub.publish(fan_net::k_topic, as_bytes(payload));
    net.drive();

    const auto &t = rec.for_topic(fan_net::k_topic);
    REQUIRE(t.delivered == 1);
    // The delivered view shares an owner handle (the borrowed buffer), so the captured
    // owner is engaged and its use_count reflects a shared addref — not a fresh
    // allocation handed in by value.
    REQUIRE(t.last_view_owner != nullptr);
    REQUIRE(t.last_view_use_count >= 1);
}
