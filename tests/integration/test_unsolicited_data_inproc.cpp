#include "test_session_stale_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/message_info.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

using namespace session_stale_fixture;
using plexus::wire::msg_type;

namespace {

constexpr std::string_view k_topic = "/unsolicited/topic";

struct clock_reset
{
    clock_reset() noexcept
    {
        manual_clock::reset();
    }
};

struct drop_log final : plexus::io::observer
{
    void on_drop(const plexus::io::detail::drop_event &ev) override
    {
        events.push_back(ev);
    }

    std::vector<plexus::io::detail::drop_event> events;
};

std::int32_t frames_of(const tapping_transport &t, msg_type type)
{
    std::int32_t n = 0;
    for(std::size_t i = 0; i < t.relay_count(); ++i)
        n += static_cast<std::int32_t>(std::ranges::count(t.relay_at(i).sent(), type, &sent_frame::type));
    return n;
}

// Publisher a, subscriber b; b dials a, so everything b sends passes b's tap.
struct unsolicited_pair
{
    clock_reset reset;
    stale_pair net;
    drop_log at_b;
    std::int32_t received;
    std::string payload;

    unsolicited_pair()
            : reset()
            , net()
            , at_b()
            , received(0)
            , payload("data")
    {
        net.b.add_observer(at_b);
        net.b.on_message_route([this](std::string_view, std::span<const std::byte>, const plexus::io::message_info &) { ++received; });
    }

    void subscribe()
    {
        net.b.note_peer(net.id_a, net.ep_a);
        net.b.subscribe(net.id_a, k_topic);
        net.ex.drain();
        REQUIRE(net.b.is_connected(net.id_a));
        REQUIRE(a_holds_b());
    }

    void unsubscribe_lost()
    {
        net.transport_b.silence(mute::unsubscribes);
        net.b.unsubscribe(net.id_a, k_topic);
        net.ex.drain();
    }

    void publish(std::int32_t times, std::chrono::nanoseconds spacing = std::chrono::nanoseconds{0})
    {
        for(std::int32_t i = 0; i < times; ++i)
        {
            net.a.publish(k_topic, as_bytes(payload));
            net.advance(spacing);
        }
    }

    bool a_holds_b()
    {
        return net.a.messages().has_remote_demand(plexus::wire::fqn_topic_hash(k_topic), nullptr);
    }

    std::int32_t unsubscribes() const
    {
        return frames_of(net.transport_b, msg_type::unsubscribe);
    }
};

}

TEST_CASE("unsolicited data: a lost unsubscribe is healed by the data that follows it", "[integration][unsolicited]")
{
    unsolicited_pair p;
    p.subscribe();
    p.publish(1);
    REQUIRE(p.received == 1);
    p.unsubscribe_lost();
    REQUIRE(p.unsubscribes() == 1);
    REQUIRE(p.a_holds_b());

    p.net.transport_b.silence(mute::none);
    p.publish(1);
    CHECK(p.received == 1);
    CHECK_FALSE(p.a_holds_b());

    const std::int32_t sent_before = frames_of(p.net.transport_a, msg_type::unidirectional);
    p.publish(10);
    CHECK(frames_of(p.net.transport_a, msg_type::unidirectional) == sent_before);
    CHECK(p.received == 1);
}

TEST_CASE("unsolicited data: the re-sent unsubscribe is rate limited per topic", "[integration][unsolicited]")
{
    unsolicited_pair p;
    p.subscribe();
    p.unsubscribe_lost();
    REQUIRE(p.unsubscribes() == 1);

    p.publish(100, std::chrono::milliseconds(5));
    CHECK(p.unsubscribes() == 2);
    CHECK(p.received == 0);

    p.net.advance(std::chrono::seconds(1));
    p.publish(1);
    CHECK(p.unsubscribes() == 3);
}

TEST_CASE("unsolicited data: on an ordered leg the first unsubscribe suffices", "[integration][unsolicited]")
{
    unsolicited_pair p;
    p.subscribe();
    p.net.b.unsubscribe(p.net.id_a, k_topic);
    p.net.ex.drain();
    p.publish(10);
    CHECK(p.at_b.events.empty());
    CHECK(p.unsubscribes() == 1);
    CHECK(p.received == 0);
}

TEST_CASE("unsolicited data: the drop is counted with its own cause", "[integration][unsolicited]")
{
    unsolicited_pair p;
    p.subscribe();
    p.unsubscribe_lost();
    p.publish(3);
    REQUIRE(p.at_b.events.size() == 3);
    for(const auto &ev : p.at_b.events)
    {
        CHECK(ev.cause == plexus::io::detail::drop_cause::unsolicited);
        CHECK(ev.topic_hash == plexus::wire::fqn_topic_hash(k_topic));
        CHECK(ev.peer == p.net.id_a);
    }
}

TEST_CASE("unsolicited data: a rebuilt session starts with an empty re-send table", "[integration][unsolicited]")
{
    unsolicited_pair p;
    p.subscribe();
    p.unsubscribe_lost();
    p.publish(1);
    REQUIRE(p.unsubscribes() == 2);

    p.net.b.session_for(p.net.id_a)->tear_down();
    p.net.transport_b.silence(mute::none);
    p.net.tick_until([&p] { return p.net.b.is_connected(p.net.id_a); }, std::chrono::milliseconds(500));
    p.subscribe();
    p.unsubscribe_lost();
    REQUIRE(p.unsubscribes() == 3);

    p.publish(1);
    CHECK(p.unsubscribes() == 4);
}
