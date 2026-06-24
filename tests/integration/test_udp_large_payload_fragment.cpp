#include "test_udp_large_payload_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace udp_large_payload_fixture;

namespace {

// An observing relay between the udp client and server: it decodes every best-effort
// fragment datagram's wire sub-header (msg_id / frag_idx / frag_cnt) before forwarding
// it, so a test can assert ON THE WIRE that a large message spans many uint32 fragments
// under ONE uint16 msg_id and that consecutive messages' msg_ids advance by exactly one
// (never wrapping the uint16). It forwards both directions verbatim; loss is irrelevant
// to the assertion (the observed headers carry the true counts regardless of delivery).
struct fragment_observer
{
    ::asio::io_context          &io;
    ::asio::ip::udp::socket      front; // faces the client
    ::asio::ip::udp::socket      back;  // faces the server
    ::asio::ip::udp::endpoint    server_ep;
    ::asio::ip::udp::endpoint    client_ep;
    ::asio::ip::udp::endpoint    from;
    std::array<std::byte, 70000> front_buf{};
    std::array<std::byte, 70000> back_buf{};

    std::vector<std::uint16_t> msg_ids; // distinct msg_ids in first-seen order
    std::uint32_t              max_frag_cnt{0};

    fragment_observer(::asio::io_context &ctx, std::uint16_t server_port)
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

    void note_fragment(std::span<const std::byte> dg)
    {
        auto outer = pwire::unwrap_udp(dg);
        if(!outer || !outer->fragmented)
            return; // handshake / whole datagram: not a fragment
        auto h = pwire::decode_udp_fragment_header(outer->frame);
        if(!h)
            return;
        max_frag_cnt = std::max(max_frag_cnt, h->frag_cnt);
        if(msg_ids.empty() || msg_ids.back() != h->msg_id)
            if(std::find(msg_ids.begin(), msg_ids.end(), h->msg_id) == msg_ids.end())
                msg_ids.push_back(h->msg_id);
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
                                     note_fragment(dg);
                                     auto copy = std::make_shared<std::vector<std::byte>>(dg.begin(), dg.end());
                                     back.async_send_to(::asio::buffer(*copy), server_ep, [copy](std::error_code, std::size_t) {});
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
                                    {
                                        auto copy = std::make_shared<std::vector<std::byte>>(back_buf.data(), back_buf.data() + n);
                                        front.async_send_to(::asio::buffer(*copy), client_ep, [copy](std::error_code, std::size_t) {});
                                    }
                                    recv_back();
                                });
    }
};

}

TEST_CASE("udp_large_payload: a 16 MB datagram message spans many uint32 fragments under ONE "
          "uint16 msg_id; consecutive messages advance the msg_id by one without wrapping",
          "[udp_large_payload][envelope16][msgid]")
{
    // The EXPLICIT msg_id-wrap assertion. A single large message is split into many
    // fragments; the fragment COUNT is a uint32 field (lifted this phase), while msg_id
    // stays uint16 and advances per-MESSAGE, never per-fragment. At a 128-byte fragment
    // budget a 16 MiB message is ~131072 fragments — far past the uint16 max (65535) — so a
    // single message's fragment count CANNOT be expressed in a uint16 msg_id: the old
    // confusion (that msg_id bounds the per-message fragment count) is closed here on the
    // wire. Two consecutive messages are sent best-effort through an observing relay that
    // decodes each fragment header; the assertion reads the observed headers (loss does not
    // matter — the headers carry the true counts):
    //   * every observed fragment of the run carries frag_cnt > 0xFFFF (a single message's
    //     fragment count exceeds what a uint16 could ever hold), proving the uint32 widening
    //     is load-bearing;
    //   * exactly TWO distinct msg_ids are observed (one per message), and they differ by
    //     exactly one — msg_id advances per-message and does not wrap/collide.
    constexpr std::size_t budget     = 128u;                // the fragment floor -> the largest count
    constexpr std::size_t payload    = 16u * 1024u * 1024u; // ~131072 fragments per message
    constexpr std::size_t ceiling    = 20u * 1024u * 1024u;
    constexpr std::size_t reassembly = 48u * 1024u * 1024u;

    ::asio::io_context   io;
    pasio::udp_transport server{io,
                                budget,
                                pasio::udp_transport::arq_type::default_ladder,
                                large_arq(),
                                pio::congestion::block,
                                pasio::detail::udp_inbound_demux::default_max_peers,
                                pasio::udp_server::default_so_sndbuf,
                                pasio::udp_server::default_so_rcvbuf,
                                pasio::udp_server::default_send_queue_bytes,
                                ceiling,
                                reassembly};
    pasio::udp_transport client{io,
                                budget,
                                fast_hs,
                                large_arq(),
                                pio::congestion::block,
                                pasio::detail::udp_inbound_demux::default_max_peers,
                                pasio::udp_server::default_so_sndbuf,
                                pasio::udp_server::default_so_rcvbuf,
                                pasio::udp_server::default_send_queue_bytes,
                                ceiling,
                                reassembly};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    server.on_accepted(
            [&](std::unique_ptr<pasio::udp_channel> ch)
            {
                accepted = std::move(ch);
                accepted->on_data([&](std::span<const std::byte>) {}); // delivery is incidental to
                                                                       // this assertion
            });
    server.listen({"udp", "127.0.0.1:0"});
    pump_until(io, [&] { return server.port() != 0; });

    fragment_observer obs{io, server.port()};
    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
    client.dial({"udp", "127.0.0.1:" + std::to_string(obs.port())}); // best-effort: fragments are wire-visible
    pump_until(io, [&] { return dialed && accepted; });
    REQUIRE(dialed != nullptr);
    REQUIRE(accepted != nullptr);

    // Two consecutive large messages: their msg_ids must differ by exactly one.
    auto first  = make_payload(payload, 0x11);
    auto second = make_payload(payload, 0x22);
    dialed->send(first);
    pump_until(io, [&] { return obs.msg_ids.size() >= 1; }, ms{4000});
    dialed->send(second);
    pump_until(io, [&] { return obs.msg_ids.size() >= 2; }, ms{4000});

    REQUIRE(obs.max_frag_cnt > 0xFFFFu); // a single message's fragment count exceeds uint16
    REQUIRE(obs.msg_ids.size() == 2);    // exactly one msg_id per message
    const int delta = static_cast<int>(obs.msg_ids[1]) - static_cast<int>(obs.msg_ids[0]);
    REQUIRE(delta == 1); // per-message advance, no wrap/collision
}
