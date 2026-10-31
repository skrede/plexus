#include "test_serial_common.h"

#include "plexus/io/qos_rxo.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/recording_channel.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/io/reference_channel_adapter.h"
#include "plexus/io/detail/egress_channel_traits.h"

#include "plexus/stream/serial_frame.h"

#include "plexus/wire/subscribe.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

using namespace serial_fixture;

namespace {

using serial_context = pio::peer_context<pasio::serial_policy>;

constexpr std::int32_t k_runs        = 3;
constexpr std::uint32_t k_narrow_max = 512;
constexpr std::size_t k_fitting_body = 256;

serial_context production_context(::asio::io_context &io, int fd, std::string peer_name)
{
    serial_context ctx;
    ctx.channel   = adopt_channel(io, fd);
    ctx.node_name = std::move(peer_name);
    return ctx;
}

struct qos_link
{
    pty_pair pty;
    ::asio::io_context io;
    plexus::log::null_logger sink;
    serial_msg_fwd relay_messages;
    serial_msg_fwd subscriber_messages;
    serial_rpc_fwd relay_procedures;
    serial_rpc_fwd subscriber_procedures;
    serial_context relay_ctx;
    serial_context subscriber_ctx;
    serial_session relay;
    serial_session subscriber;
    std::vector<pio::qos_edge> edges;
    std::vector<wire::subscribe_status> refusals;
    std::vector<std::uint8_t> degraded;
    std::int32_t deliveries;

    qos_link()
            : relay_messages(sink)
            , subscriber_messages(sink)
            , relay_procedures(io, k_long_timeout, sink)
            , subscriber_procedures(io, k_long_timeout, sink)
            , relay_ctx(production_context(io, pty.take_master(), "subscriber"))
            , subscriber_ctx(production_context(io, pty.take_slave(), "relay"))
            , relay(relay_ctx, io, make_cfg(0x01), k_long_timeout, relay_messages, relay_procedures, true, sink)
            , subscriber(subscriber_ctx, io, make_cfg(0x02), k_long_timeout, subscriber_messages, subscriber_procedures, false, sink)
            , deliveries(0)
    {
        relay_messages.declare("default", plexus::topic_qos{});
        relay_messages.declare("narrow", plexus::topic_qos{.max_message_bytes = k_narrow_max});
        relay_messages.on_qos_change([this](const pio::qos_change_event &e) { edges.push_back(e.edge); });
        subscriber.on_message([this](std::string_view, std::span<const std::byte>) { ++deliveries; });
        subscriber.on_subscribe_refused([this](std::uint64_t, wire::subscribe_status s) { refusals.push_back(s); });
        subscriber.on_subscribe_degraded([this](std::uint64_t, std::uint8_t bits) { degraded.push_back(bits); });
        relay.start();
        subscriber.start();
        plexus::testing::pump_until(io, [this] { return relay.is_complete() && subscriber.is_complete(); }, k_pump_bound);
    }

    void subscribe(std::string_view fqn, pio::rxo_mode mode)
    {
        REQUIRE((relay.is_complete() && subscriber.is_complete()));
        subscriber.subscribe(fqn, pio::subscriber_qos{.rxo = mode});
        plexus::testing::pump_until(io, [this] { return !edges.empty(); }, k_pump_bound);
    }

    // The subscribe response precedes the data frame on the ordered line, so a delivery means every verdict arrived.
    void deliver(std::string_view fqn)
    {
        const std::vector<std::byte> body(k_fitting_body, std::byte{0x22});
        relay_messages.publish(fqn, body, relay.session_id());
        plexus::testing::pump_until(io, [this] { return deliveries > 0; }, k_pump_bound);
    }
};

}

TEST_CASE("serial qos ceiling: a default topic over the serial leg is degraded for a permissive subscriber and refused "
          "for a strict one",
          "[integration][serial][qos]")
{
    std::int32_t proven = 0;
    for(std::int32_t run = 0; run < k_runs; ++run)
    {
        {
            qos_link link;
            link.subscribe("default", pio::rxo_mode::permissive);
            link.deliver("default");
            REQUIRE(link.edges == std::vector{pio::qos_edge::degraded});
            REQUIRE(link.degraded == std::vector{pio::k_rxo_field_max_message_bytes});
            REQUIRE(link.refusals.empty());
            REQUIRE(link.deliveries == 1);
        }
        {
            qos_link link;
            link.subscribe("default", pio::rxo_mode::strict);
            plexus::testing::pump_until(link.io, [&] { return !link.refusals.empty(); }, k_pump_bound);
            REQUIRE(link.edges == std::vector{pio::qos_edge::refused});
            REQUIRE(link.refusals == std::vector{wire::subscribe_status::incompatible_qos});
            REQUIRE(link.degraded.empty());
        }
        ++proven;
    }
    REQUIRE(proven == k_runs);
}

TEST_CASE("serial qos ceiling: a topic whose max fits the serial leg is admitted clean", "[integration][serial][qos]")
{
    std::int32_t proven = 0;
    for(std::int32_t run = 0; run < k_runs; ++run)
    {
        qos_link link;
        link.subscribe("narrow", pio::rxo_mode::strict);
        link.deliver("narrow");
        REQUIRE(link.edges == std::vector{pio::qos_edge::accepted});
        REQUIRE(link.refusals.empty());
        REQUIRE(link.degraded.empty());
        REQUIRE(link.deliveries == 1);
        ++proven;
    }
    REQUIRE(proven == k_runs);
}

TEST_CASE("serial qos ceiling: the erased and recording wrappers report the serial leg's ceiling", "[integration][serial][qos]")
{
    constexpr std::size_t k_ceiling = stream::serial_frame_ceiling(stream::k_serial_default_max_payload);
    pty_pair line;
    pty_pair held_line;
    ::asio::io_context io;
    pio::polymorphic_byte_channel erased{std::make_unique<pio::channel_adapter<pasio::serial_channel>>(adopt_channel(io, line.take_master()))};
    pio::recording_channel<pasio::serial_channel> recorded{adopt_channel(io, line.take_slave())};
    const auto held = adopt_channel(io, held_line.take_master());
    pio::polymorphic_byte_channel referenced{std::make_unique<pio::reference_channel_adapter<pasio::serial_channel>>(*held)};
    CHECK(pio::detail::channel_frame_ceiling(erased) == k_ceiling);
    CHECK(pio::detail::channel_frame_ceiling(recorded) == k_ceiling);
    CHECK(pio::detail::channel_frame_ceiling(referenced) == k_ceiling);

    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex(bus);
    using erased_inproc = pio::channel_adapter<plexus::inproc::inproc_channel<>>;
    pio::polymorphic_byte_channel unprobed{std::make_unique<erased_inproc>(std::make_unique<plexus::inproc::inproc_channel<>>(ex))};
    CHECK(pio::detail::channel_frame_ceiling(unprobed) == (std::numeric_limits<std::size_t>::max)());
}
