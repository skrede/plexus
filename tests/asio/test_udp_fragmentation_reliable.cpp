#include "test_udp_fragmentation_common.h"

using namespace udp_fragmentation_fixture;

namespace {

// A drop-scripting relay between the client and the server socket: acks + handshake +
// best_effort always pass; a scripted reliable data segment is dropped once and the ARQ
// retransmits past the drop. Mirrors the reliable-datagram loss harness; the buffer is
// sized for a default-budget fragment datagram.
struct relay
{
    ::asio::io_context         &io;
    ::asio::ip::udp::socket     front;
    ::asio::ip::udp::socket     back;
    ::asio::ip::udp::endpoint   server_ep;
    ::asio::ip::udp::endpoint   client_ep;
    ::asio::ip::udp::endpoint   from;
    std::array<std::byte, 4096> front_buf{};
    std::array<std::byte, 4096> back_buf{};
    std::deque<action>          data_script;
    int                         data_seen{0};

    relay(::asio::io_context &ctx, std::uint16_t server_port)
            : io(ctx)
            , front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
    {
        recv_front();
        recv_back();
    }

    [[nodiscard]] std::uint16_t port() const
    {
        return front.local_endpoint().port();
    }

    [[nodiscard]] static bool is_data(std::span<const std::byte> dg)
    {
        auto dec = wire::unwrap_udp(dg);
        return dec && dec->kind == wire::udp_envelope_kind::reliable_arq && wire::peek_udp_arq_kind(dec->frame) == wire::udp_arq_kind::segment;
    }

    void to_server(std::span<const std::byte> dg)
    {
        std::vector<std::byte> copy(dg.begin(), dg.end());
        back.send_to(::asio::buffer(copy.data(), copy.size()), server_ep);
    }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from,
                                 [this](std::error_code ec, std::size_t n)
                                 {
                                     if(ec)
                                         return;
                                     client_ep = from;
                                     std::span<const std::byte> dg{front_buf.data(), n};
                                     if(is_data(dg))
                                     {
                                         ++data_seen;
                                         action a = action::pass;
                                         if(!data_script.empty())
                                         {
                                             a = data_script.front();
                                             data_script.pop_front();
                                         }
                                         if(a == action::pass)
                                             to_server(dg);
                                     }
                                     else
                                         to_server(dg);
                                     recv_front();
                                 });
    }

    void recv_back()
    {
        back.async_receive_from(::asio::buffer(back_buf), from,
                                [this](std::error_code ec, std::size_t n)
                                {
                                    if(ec)
                                        return;
                                    if(client_ep.port() != 0)
                                        front.send_to(::asio::buffer(back_buf.data(), n), client_ep);
                                    recv_back();
                                });
    }
};

}

TEST_CASE("udp fragment reliable: a large payload reassembles byte-equal with a lost fragment "
          "retransmitted",
          "[udp][fragment][reliable]")
{
    // Each fragment of a large reliable payload rides ABOVE the ARQ as one independently-
    // retransmitted segment. A scripted single-fragment loss is selectively retransmitted
    // and the message completes byte-equal end-to-end. The payload is sized so the fragment
    // count stays inside the ARQ window + the bounded congestion=block queue (no synchronous
    // overrun of the unsized window — the throughput interaction above that bound is a
    // separate flow-control concern handled elsewhere).
    constexpr std::size_t budget       = 512;
    constexpr int         k_iterations = 10;
    int                   proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context   io;
        pasio::udp_transport server{io, budget, pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, budget, fast_hs, fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::vector<std::byte>> got;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b) { got.emplace_back(b.begin(), b.end()); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        relay link{io, server.port()};
        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(link.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);
        REQUIRE(dialed->mode() == plexus::datagram::detail::udp_channel_mode::reliable_datagram);

        // Drop the 3rd reliable data segment once: the ARQ retransmits it and the receiver
        // HOL-holds the later fragments behind the gap until the retransmit fills it.
        link.data_script = {action::pass, action::pass, action::drop};

        // ~24 KiB at a 512-byte budget -> ~50 fragments, inside the 256-segment window.
        auto payload = make_payload(24 * 1024, static_cast<std::uint8_t>(iter));
        REQUIRE(payload.size() + wire::udp_envelope_overhead + 1 > budget); // genuinely oversize
        dialed->send(payload);

        pump_until(io, [&] { return !got.empty(); });
        REQUIRE(got.size() == 1);                   // ONE reassembled message
        REQUIRE(equal_bytes(got.front(), payload)); // byte-equal over loss
        REQUIRE(link.data_seen >= 4);               // genuinely lossy (>=1 retransmit)
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
