// A large reliable send is admitted whole or refused whole: every fragment goes to the ARQ window
// or the backpressure queue, or none does, so the peer never waits on a message with a hole in it.
#include "test_udp_fragmentation_common.h"

#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/detail/drop_event.h"

#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

using namespace udp_fragmentation_fixture;

namespace {

using submit_result = pasio::udp_channel::submit_result;

constexpr std::size_t k_budget  = 256;
constexpr std::size_t k_slice   = pio::effective_fragment_budget(k_budget);
constexpr std::size_t k_frame   = k_slice + wire::udp_fragment_subheader;
constexpr std::size_t k_payload = 6 * k_slice;

// The per-message ceiling is the payload itself, so the queue's cap (floored at that ceiling) is
// one message; the configured backpressure bytes sit below it.
pasio::udp_transport make_transport(::asio::io_context &io, pio::congestion cong, std::size_t window, pasio::udp_transport::arq_type::schedule hs)
{
    const plexus::datagram::detail::udp_arq_config arq{.window = window, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{120}, .max_retransmit = 20};
    return pasio::udp_transport{io,
                                k_budget,
                                hs,
                                arq,
                                cong,
                                pasio::detail::udp_inbound_demux::default_max_peers,
                                pasio::udp_server::default_so_sndbuf,
                                pasio::udp_server::default_so_rcvbuf,
                                pasio::udp_server::default_send_queue_bytes,
                                k_payload,
                                pio::reassembly_memory_budget,
                                1};
}

struct admission_pair
{
    int would_block;
    int fragments_seen;
    ::asio::io_context io;
    pasio::udp_transport client;
    pasio::udp_transport server;
    std::vector<std::vector<std::byte>> got;
    std::vector<pio::detail::drop_cause> drops;
    std::unique_ptr<pasio::udp_channel> dialed;
    std::unique_ptr<pasio::udp_channel> accepted;

    admission_pair(pio::congestion cong, std::size_t window)
            : would_block(0)
            , fragments_seen(0)
            , io()
            , client(make_transport(io, cong, window, fast_hs))
            , server(make_transport(io, cong, window, pasio::udp_transport::arq_type::default_ladder))
            , got()
            , drops()
            , dialed()
            , accepted()
    {
        server.on_accepted([this](std::unique_ptr<pasio::udp_channel> ch) { adopt_accepted(std::move(ch)); });
        server.listen({"udp", "127.0.0.1:0"});
        plexus::testing::pump_until(io, [this] { return server.port() != 0; }, k_pump_bound);
        client.on_dialed([this](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { adopt_dialed(std::move(ch)); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
        plexus::testing::pump_until(io, [this] { return dialed && accepted; }, k_pump_bound);
    }

    void adopt_accepted(std::unique_ptr<pasio::udp_channel> ch)
    {
        accepted = std::move(ch);
        accepted->on_data([this](std::span<const std::byte> b) { got.emplace_back(b.begin(), b.end()); });
        accepted->on_reliable_segment([this](std::uint16_t, std::span<const std::byte> p) { fragments_seen += p.size() == k_frame ? 1 : 0; });
    }

    void adopt_dialed(std::unique_ptr<pasio::udp_channel> ch)
    {
        dialed = std::move(ch);
        dialed->on_drop([this](const pio::detail::drop_event &ev) { drops.push_back(ev.cause); });
        dialed->on_error([this](pio::io_error e) { would_block += e == pio::io_error::would_block ? 1 : 0; });
    }
};

std::vector<std::byte> small(std::size_t n)
{
    return std::vector<std::byte>(n, std::byte{0x5A});
}

}

TEST_CASE("udp reliable large send: a message whose fragments do not all fit the window is refused whole under drop_newest", "[udp][fragment][congestion]")
{
    admission_pair f{pio::congestion::drop_newest, 4};
    REQUIRE(f.dialed != nullptr);
    REQUIRE(f.dialed->send_reliable(make_payload(k_payload, 1)) == submit_result::window_full);
    REQUIRE(f.drops == std::vector{pio::detail::drop_cause::arq_shed});
    REQUIRE(f.dialed->dropped_count() == 1);
    REQUIRE(f.dialed->send_reliable(small(8)) == submit_result::admitted);
    plexus::testing::pump_until(f.io, [&] { return !f.got.empty(); }, k_pump_bound);
    REQUIRE(f.got == std::vector<std::vector<std::byte>>{small(8)});
    REQUIRE(f.fragments_seen == 0);
}

TEST_CASE("udp reliable large send: a message whose overflow fits the backpressure cap is admitted whole under block", "[udp][fragment][congestion]")
{
    admission_pair f{pio::congestion::block, 4};
    REQUIRE(f.dialed != nullptr);
    const auto payload = make_payload(k_payload, 2);
    REQUIRE(f.dialed->send_reliable(payload) == submit_result::admitted);
    REQUIRE(f.dialed->backpressured() == 2 * k_frame);
    plexus::testing::pump_until(f.io, [&] { return !f.got.empty(); }, k_pump_bound);
    REQUIRE(f.got.size() == 1);
    REQUIRE(equal_bytes(f.got.front(), payload));
    REQUIRE(f.dialed->backpressured() == 0);
    REQUIRE(f.would_block == 0);
}

TEST_CASE("udp reliable large send: a message whose overflow misses the backpressure cap queues nothing under block", "[udp][fragment][congestion]")
{
    admission_pair f{pio::congestion::block, 4};
    REQUIRE(f.dialed != nullptr);
    std::size_t sent = 0;
    for(; sent < 4; ++sent)
        REQUIRE(f.dialed->send_reliable(small(8)) == submit_result::admitted);
    // Park whole messages until the queue's free room holds one full fragment and no more.
    for(std::size_t left = f.dialed->write_queue_capacity() - (k_frame + 1); left != 0; ++sent)
    {
        const std::size_t n = std::min<std::size_t>(left, 200);
        REQUIRE(f.dialed->send_reliable(small(n)) == submit_result::admitted);
        left -= n;
    }
    const std::size_t parked = f.dialed->backpressured();
    REQUIRE(f.dialed->send_reliable(make_payload(k_payload, 3)) == submit_result::window_full);
    REQUIRE(f.would_block == 1);
    REQUIRE(f.dialed->backpressured() == parked);
    plexus::testing::pump_until(f.io, [&] { return f.got.size() == sent && f.dialed->backpressured() == 0; }, k_pump_bound);
    REQUIRE(f.got.size() == sent);
    REQUIRE(f.fragments_seen == 0);
}

TEST_CASE("udp reliable large send: a message that fits the window is admitted as before", "[udp][fragment][congestion]")
{
    admission_pair f{pio::congestion::drop_newest, 8};
    REQUIRE(f.dialed != nullptr);
    const auto payload = make_payload(k_payload, 4);
    REQUIRE(f.dialed->send_reliable(payload) == submit_result::admitted);
    plexus::testing::pump_until(f.io, [&] { return !f.got.empty(); }, k_pump_bound);
    REQUIRE(f.got.size() == 1);
    REQUIRE(equal_bytes(f.got.front(), payload));
    REQUIRE(f.drops.empty());
    REQUIRE(f.fragments_seen >= 6);
}

TEST_CASE("udp fragment count: fragment_count and fragment_tail_size agree with what split emits", "[udp][fragment]")
{
    for(const bool aead : {false, true})
        for(const std::size_t budget : {std::size_t{0}, k_budget, pasio::udp_channel::default_max_payload})
        {
            const std::size_t slice = pio::effective_fragment_budget(budget, aead);
            for(const std::size_t size : {std::size_t{0}, std::size_t{1}, slice - 1, slice, slice + 1, 6 * slice, 6 * slice + 1})
            {
                std::vector<std::size_t> slices;
                pio::fragment_sink sink = [&slices](std::uint32_t, std::uint32_t, std::span<const std::byte> s) { slices.push_back(s.size()); };
                const std::vector<std::byte> payload(size);
                pio::split(payload, budget, 0, sink, aead);
                REQUIRE(pio::fragment_count(size, budget, aead) == slices.size());
                REQUIRE(pio::fragment_tail_size(size, budget, slices.size(), aead) == 0);
                std::size_t tail = 0;
                for(std::size_t first = slices.size(); first != 0; --first)
                {
                    tail += slices[first - 1];
                    REQUIRE(pio::fragment_tail_size(size, budget, first - 1, aead) == tail);
                }
            }
        }
}
