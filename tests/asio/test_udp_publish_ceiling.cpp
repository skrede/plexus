#include "test_udp_transport_common.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/io/io_error.h"
#include "plexus/io/message_forwarder.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/testing/pump.h"

#include <algorithm>

using namespace udp_transport_fixture;

namespace {

using udp_msg_fwd = pio::message_forwarder<pasio::udp_policy>;

constexpr std::size_t k_fitting_body = 64;

bool is_unidirectional(std::span<const std::byte> frame)
{
    const auto header = wire::decode_header(frame);
    return header.has_value() && header->type == wire::msg_type::unidirectional;
}

struct publish_rig
{
    ::asio::io_context io;
    plexus::log::null_logger sink;
    pasio::udp_transport server;
    pasio::udp_transport client;
    udp_msg_fwd messages;
    std::unique_ptr<pasio::udp_channel> accepted;
    std::unique_ptr<pasio::udp_channel> dialed;
    std::vector<pio::io_error> client_errors;
    std::int32_t unidirectional_frames;
    std::int32_t sender_drops;

    publish_rig()
            : server(io)
            , client(io)
            , messages(sink)
            , unidirectional_frames(0)
            , sender_drops(0)
    {
        listen();
        dial();
        messages.declare("ceiling", plexus::topic_qos{});
        messages.on_drop([this](const pio::detail::drop_event &e) { sender_drops += (e.cause == pio::detail::drop_cause::oversize); });
    }

    void listen()
    {
        server.on_accepted(
                [this](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([this](std::span<const std::byte> b) { unidirectional_frames += is_unidirectional(b); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until([this] { return server.port() != 0; });
    }

    void dial()
    {
        client.on_dialed(
                [this](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                {
                    dialed = std::move(ch);
                    dialed->on_error([this](pio::io_error e) { client_errors.push_back(e); });
                });
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until([this] { return dialed != nullptr && accepted != nullptr; });
    }

    void publish(std::size_t body_size)
    {
        const std::vector<std::byte> body(body_size, std::byte{0x5A});
        messages.publish("ceiling", body);
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        plexus::testing::pump_until(io, pred, std::chrono::seconds(5));
    }
};

}

TEST_CASE("udp publish ceiling: a publish over the leg's max message size is shed at the sender's egress and never "
          "reaches the channel",
          "[udp][transport]")
{
    publish_rig rig;
    REQUIRE(rig.dialed != nullptr);
    REQUIRE(rig.messages.attach_for_fanout(udp_msg_fwd::peer{*rig.dialed, "acceptor"}, "ceiling"));

    rig.publish(pio::global_default_max_message_bytes + 1);
    rig.publish(k_fitting_body);
    rig.pump_until([&] { return rig.unidirectional_frames > 0; });

    REQUIRE(std::count(rig.client_errors.begin(), rig.client_errors.end(), pio::io_error::message_too_large) == 0);
    REQUIRE(rig.sender_drops == 1);
    REQUIRE(rig.unidirectional_frames == 1);
}
