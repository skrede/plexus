#include "test_serial_common.h"

#include "plexus/io/detail/drop_event.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

using namespace serial_fixture;

namespace {

using serial_context = pio::peer_context<pasio::serial_policy>;

constexpr std::int32_t k_runs         = 3;
constexpr std::size_t k_oversize_body = 2048;
constexpr std::size_t k_fitting_body  = 512;

serial_context production_context(::asio::io_context &io, int fd, std::string peer_name)
{
    serial_context ctx;
    ctx.channel   = adopt_channel(io, fd);
    ctx.node_name = std::move(peer_name);
    return ctx;
}

struct serial_link
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
    std::int32_t sender_drops;
    std::int32_t receiver_frame_drops;
    std::int32_t receiver_deliveries;

    serial_link()
            : sender_messages(sink)
            , receiver_messages(sink)
            , sender_procedures(io, k_long_timeout, sink)
            , receiver_procedures(io, k_long_timeout, sink)
            , sender_ctx(production_context(io, pty.take_master(), "receiver"))
            , receiver_ctx(production_context(io, pty.take_slave(), "sender"))
            , sender(sender_ctx, io, make_cfg(0x01), k_long_timeout, sender_messages, sender_procedures, false, sink)
            , receiver(receiver_ctx, io, make_cfg(0x02), k_long_timeout, receiver_messages, receiver_procedures, true, sink)
            , sender_drops(0)
            , receiver_frame_drops(0)
            , receiver_deliveries(0)
    {
        receiver_messages.declare("sized", plexus::topic_qos{});
        receiver.on_message([this](std::string_view, std::span<const std::byte>) { ++receiver_deliveries; });
        receiver_ctx.channel->on_frame_dropped([this](wire::close_cause) { ++receiver_frame_drops; });
        sender_messages.on_drop([this](const pio::detail::drop_event &e) { sender_drops += (e.cause == pio::detail::drop_cause::oversize); });
    }

    bool complete() const
    {
        return sender.is_complete() && receiver.is_complete();
    }

    void publish(std::size_t body_size)
    {
        const std::vector<std::byte> body(body_size, std::byte{0x11});
        sender_messages.publish("sized", body, sender.session_id());
    }
};

}

TEST_CASE("serial oversize: a publish over the leg ceiling is refused at the sender and never reaches the wire, a "
          "fitting one is delivered",
          "[integration][serial][oversize]")
{
    std::int32_t proven = 0;
    for(std::int32_t run = 0; run < k_runs; ++run)
    {
        serial_link link;
        link.sender.start();
        link.receiver.start();
        plexus::testing::pump_until(link.io, [&] { return link.complete(); }, k_pump_bound);
        REQUIRE(link.complete());
        REQUIRE(link.sender_messages.attach_for_fanout(serial_msg_fwd::peer{link.sender.msg_peer().channel, "receiver"}, "sized"));

        link.publish(k_oversize_body);
        link.publish(k_fitting_body);
        plexus::testing::pump_until(link.io, [&] { return link.receiver_deliveries > 0; }, k_pump_bound);

        REQUIRE(link.receiver_frame_drops == 0);
        REQUIRE(link.sender_drops == 1);
        REQUIRE(link.receiver_deliveries == 1);
        REQUIRE(link.complete());
        ++proven;
    }
    REQUIRE(proven == k_runs);
}
