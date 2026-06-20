#include "test_udp_reliable_datagram_common.h"

using namespace udp_reliable_datagram_fixture;

namespace {

// A drop-scripting relay (acks + handshake always pass; a once-dropped data seq is
// retransmitted past the drop). The same shape as the ARQ loss harness.
struct relay
{
    ::asio::io_context         &io;
    ::asio::ip::udp::socket     front;
    ::asio::ip::udp::socket     back;
    ::asio::ip::udp::endpoint   server_ep;
    ::asio::ip::udp::endpoint   client_ep;
    ::asio::ip::udp::endpoint   from;
    std::array<std::byte, 2048> front_buf{};
    std::array<std::byte, 2048> back_buf{};
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

    [[nodiscard]] std::uint16_t port() const { return front.local_endpoint().port(); }

    [[nodiscard]] static bool is_data(std::span<const std::byte> dg)
    {
        auto dec = wire::unwrap_udp(dg);
        return dec && dec->kind == wire::udp_envelope_kind::reliable_arq &&
                wire::peek_udp_arq_kind(dec->frame) == wire::udp_arq_kind::segment;
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
                                        front.send_to(::asio::buffer(back_buf.data(), n),
                                                      client_ep);
                                    recv_back();
                                });
    }
};

}

TEST_CASE("udp reliable_datagram: a 'udpr' channel delivers in-order over injected loss via send()",
          "[udp][reliable_datagram]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context   io;
        pasio::udp_transport server{io, pasio::udp_channel::default_max_payload,
                                    pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs,
                                    fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::string>            delivered;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b)
                                      { delivered.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        relay link{io, server.port()};
        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(link.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        // Drop the 2nd segment once: send() (reliable mode) must retransmit + the receiver
        // HOL holds 3,4 behind the gap until the retransmit fills it — all 4 in order.
        link.data_script = {action::pass, action::drop, action::pass, action::pass};

        std::vector<std::string> sent;
        for(int i = 0; i < 4; ++i)
        {
            const std::string p = "udpr-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            dialed->send(bytes_of(p)); // the SINGLE send verb drives the ARQ in reliable mode
        }
        pump_until(io, [&] { return delivered.size() == 4; });

        REQUIRE(delivered == sent);   // exactly once, in publish order, over loss
        REQUIRE(link.data_seen >= 5); // genuinely lossy (>=1 retransmit)
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
