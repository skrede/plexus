#include "test_serial_common.h"

#include "plexus/io/io_error.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

using namespace serial_fixture;

namespace {

using serial_context = pio::peer_context<pasio::serial_policy>;

constexpr std::int32_t k_runs        = 3;
constexpr std::size_t k_fitting_body = 512;

std::vector<std::byte> frame_of(std::size_t payload_len)
{
    const std::vector<std::byte> payload(payload_len, std::byte{0x11});
    return wire::encode_frame(wire::frame_header{.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0}, payload);
}

struct refusal_probe
{
    explicit refusal_probe(pasio::serial_channel &tx, pasio::serial_channel &rx)
            : tx_oversize_drops(0)
            , rx_frame_drops(0)
            , tx_errors()
            , delivered()
    {
        tx.on_error([this](pio::io_error e) { tx_errors.push_back(e); });
        tx.on_drop([this](const pio::detail::drop_event &e) { tx_oversize_drops += (e.cause == pio::detail::drop_cause::oversize); });
        rx.on_data([this](std::span<const std::byte> f) { delivered.emplace_back(f.begin(), f.end()); });
        rx.on_frame_dropped([this](wire::close_cause) { ++rx_frame_drops; });
    }

    std::int32_t tx_oversize_drops;
    std::int32_t rx_frame_drops;
    std::vector<pio::io_error> tx_errors;
    std::vector<std::vector<std::byte>> delivered;
};

serial_context production_context(::asio::io_context &io, int fd, std::string peer_name)
{
    serial_context ctx;
    ctx.channel   = adopt_channel(io, fd);
    ctx.node_name = std::move(peer_name);
    return ctx;
}

struct session_link
{
    pty_pair pty;
    ::asio::io_context io;
    plexus::log::null_logger sink;
    serial_msg_fwd sender_messages;
    serial_msg_fwd receiver_messages;
    serial_rpc_fwd sender_procedures;
    serial_rpc_fwd receiver_procedures;
    serial_context sender_ctx;
    serial_context receiver_ctx;
    serial_session sender;
    serial_session receiver;
    std::int32_t transport_drops;
    std::int32_t sender_oversize_drops;
    std::int32_t receiver_frame_drops;
    std::int32_t receiver_deliveries;

    session_link()
            : sender_messages(sink)
            , receiver_messages(sink)
            , sender_procedures(io, k_long_timeout, sink)
            , receiver_procedures(io, k_long_timeout, sink)
            , sender_ctx(production_context(io, pty.take_master(), "receiver"))
            , receiver_ctx(production_context(io, pty.take_slave(), "sender"))
            , sender(sender_ctx, io, make_cfg(0x01), k_long_timeout, sender_messages, sender_procedures, false, sink)
            , receiver(receiver_ctx, io, make_cfg(0x02), k_long_timeout, receiver_messages, receiver_procedures, true, sink)
            , transport_drops(0)
            , sender_oversize_drops(0)
            , receiver_frame_drops(0)
            , receiver_deliveries(0)
    {
        receiver_messages.declare("sized", plexus::topic_qos{});
        receiver.on_message([this](std::string_view, std::span<const std::byte>) { ++receiver_deliveries; });
        receiver_ctx.channel->on_frame_dropped([this](wire::close_cause) { ++receiver_frame_drops; });
        sender_ctx.channel->on_drop([this](const pio::detail::drop_event &e) { sender_oversize_drops += (e.cause == pio::detail::drop_cause::oversize); });
        sender.on_transport_drop([this] { ++transport_drops; });
        receiver.on_transport_drop([this] { ++transport_drops; });
    }

    bool complete() const
    {
        return sender.is_complete() && receiver.is_complete();
    }
};

}

TEST_CASE("serial send refusal: a frame of exactly the leg ceiling is written and one byte more is refused at the sender", "[integration][serial][refusal]")
{
    std::int32_t proven = 0;
    for(std::int32_t run = 0; run < k_runs; ++run)
    {
        pty_pair pty;
        ::asio::io_context io;
        auto tx = adopt_channel(io, pty.take_master());
        auto rx = adopt_channel(io, pty.take_slave());
        refusal_probe probe{*tx, *rx};
        const auto ceiling_frame = frame_of(stream::k_serial_default_max_payload);
        REQUIRE(ceiling_frame.size() == tx->max_frame_bytes());

        tx->send(ceiling_frame);
        tx->send(frame_of(stream::k_serial_default_max_payload + 1));
        plexus::testing::pump_until(io, [&] { return !probe.delivered.empty(); }, k_pump_bound);
        settle(io);

        REQUIRE(probe.delivered == std::vector<std::vector<std::byte>>{ceiling_frame});
        REQUIRE(probe.tx_errors == std::vector<pio::io_error>{pio::io_error::message_too_large});
        REQUIRE(probe.tx_oversize_drops == 1);
        REQUIRE(probe.rx_frame_drops == 0);
        ++proven;
    }
    REQUIRE(proven == k_runs);
}

TEST_CASE("serial send refusal: a refused send keeps both sessions and the fitting traffic", "[integration][serial][refusal]")
{
    std::int32_t proven = 0;
    for(std::int32_t run = 0; run < k_runs; ++run)
    {
        session_link link;
        link.sender.start();
        link.receiver.start();
        plexus::testing::pump_until(link.io, [&] { return link.complete(); }, k_pump_bound);
        REQUIRE(link.complete());
        REQUIRE(link.sender_messages.attach_for_fanout(serial_msg_fwd::peer{link.sender.msg_peer().channel, "receiver"}, "sized"));
        REQUIRE(link.receiver_messages.attach(link.receiver.msg_peer(), "sized"));

        link.sender_ctx.channel->send(frame_of(stream::k_serial_default_max_payload + 1));
        const std::vector<std::byte> body(k_fitting_body, std::byte{0x22});
        link.sender_messages.publish("sized", body, link.sender.session_id());
        plexus::testing::pump_until(link.io, [&] { return link.receiver_deliveries > 0; }, k_pump_bound);
        settle(link.io);

        REQUIRE(link.transport_drops == 0);
        REQUIRE(link.sender_oversize_drops == 1);
        REQUIRE(link.receiver_frame_drops == 0);
        REQUIRE(link.receiver_deliveries == 1);
        REQUIRE(link.complete());
        ++proven;
    }
    REQUIRE(proven == k_runs);
}
