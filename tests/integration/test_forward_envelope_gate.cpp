// The splice-time envelope gate: a forwarded frame too large for the outbound leg's
// per-message ceiling drops-with-count at the relay instead of being sent and torn down at the narrow
// peer's reassembler. Two proofs over REAL asymmetric-limit legs:
//
//   1. Serial narrow leg (small per-message ceiling over an openpty serial link): a relay fanning an
//      OVERSIZED forwarded frame toward the narrow leg drops it with a splice_oversize count and never
//      puts it on the wire — the narrow peer's session stays COMPLETE (no teardown/redial). A
//      fitting-but-still-real frame on the same leg transits and delivers, so the drop is size-selective
//      over a genuinely live wire, not a dead one. A channel that publishes no ceiling probe is
//      unlimited, so the gate is inert on it (the capability probe is asserted directly).
//
//   2. UDP reassemble-then-refragment across a relay with ASYMMETRIC fragment budgets: a large,
//      genuinely MULTI-fragment payload arrives whole on the wide inbound leg (the channel reassembles
//      within its bounded buffer), is re-sent on a NARROWER outbound leg (refragmented to the smaller
//      budget), and reassembles BYTE-FOR-BYTE at the consumer. A one-fragment payload would prove
//      nothing and is rejected by construction (payload strictly exceeds both budgets).

#include "test_serial_common.h"

#include "plexus/io/detail/forward_splice.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/asio/udp_transport.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <limits>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

using namespace serial_fixture;

namespace {

using splice = pio::forward_splice<pasio::serial_policy>;

plexus::node_id id_of(std::uint8_t base)
{
    plexus::node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(base + i);
    return id;
}

// A header-on unidirectional frame for a topic with a body of the chosen size, captured off a real
// producer's wire (inproc) — the exact inner a relay splices inside a forwarded envelope.
std::vector<std::byte> capture_inner(std::string_view fqn, std::size_t body_size)
{
    using plexus::inproc::inproc_bus;
    using plexus::inproc::inproc_executor;
    using plexus::inproc::inproc_channel;
    using inproc_fwd = pio::message_forwarder<plexus::inproc::inproc_policy>;

    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> sub(ex);
    inproc_channel<> cap(ex);
    sub.connect_to(cap.local_endpoint());
    std::vector<std::byte> framed;
    cap.on_data([&](std::span<const std::byte> f) { framed.assign(f.begin(), f.end()); });

    plexus::log::null_logger sink;
    inproc_fwd producer{sink};
    producer.declare(fqn, plexus::topic_qos{});
    producer.attach_for_fanout(inproc_fwd::peer{sub, "producer"}, fqn);
    ex.drain();
    std::vector<std::byte> body(body_size, std::byte{0xA5});
    producer.publish(fqn, body);
    ex.drain();
    return framed;
}

}

TEST_CASE("envelope gate: an oversized forwarded frame drops-with-count at the splice and leaves the "
          "narrow serial session complete; a fitting frame on the same leg transits",
          "[integration][forward][envelope][serial]")
{
    constexpr int k_runs        = 3; // a serial round-trip claim is never made from a single run
    constexpr std::size_t k_ceiling = 4096;

    for(int run = 0; run < k_runs; ++run)
    {
        pty_pair pty;
        ::asio::io_context io;
        plexus::log::null_logger sink;
        serial_msg_fwd relay_messages{sink};
        serial_msg_fwd peer_messages{sink};
        serial_rpc_fwd relay_procedures{io, k_long_timeout, sink};
        serial_rpc_fwd peer_procedures{io, k_long_timeout, sink};

        const auto cfg_limited = stream::with_message_limits(stream::stream_inbound_config{}, k_ceiling, k_ceiling + wire::header_size);
        pio::peer_context<pasio::serial_policy> relay_ctx;
        pio::peer_context<pasio::serial_policy> peer_ctx;
        relay_ctx.channel   = adopt_channel(io, pty.take_master(), cfg_limited);
        relay_ctx.node_name = "narrow";
        peer_ctx.channel    = adopt_channel(io, pty.take_slave(), cfg_limited);
        peer_ctx.node_name  = "relay";
        serial_session relay{relay_ctx, io, make_cfg(0x01), k_long_timeout, relay_messages, relay_procedures, /*is_inbound_bootstrap=*/false, sink};
        serial_session peer{peer_ctx, io, make_cfg(0x02), k_long_timeout, peer_messages, peer_procedures, /*is_inbound_bootstrap=*/true, sink};

        // The narrow peer resolves the relayed topic so a delivered forwarded frame is observable.
        peer_messages.declare("gated", plexus::topic_qos{});
        std::size_t peer_deliveries = 0;
        peer.on_message([&](std::string_view, std::span<const std::byte>) { ++peer_deliveries; });

        // The splice's drop-with-count surfaces as an append-only drop_event row carrying the cause.
        std::size_t oversize_drops = 0;
        relay_messages.on_drop([&](const pio::detail::drop_event &e) { oversize_drops += (e.cause == pio::detail::drop_cause::splice_oversize); });

        relay.start();
        peer.start();
        pump_until(io, [&] { return relay.is_complete() && peer.is_complete(); });
        REQUIRE(relay.is_complete());
        REQUIRE(peer.is_complete());

        // The relay serves the topic over the narrow leg (the exact channel a downstream demand would
        // fan onto). The probe reports the configured ceiling; an unprobed inproc channel is unlimited.
        REQUIRE(relay.msg_peer().channel.max_frame_bytes() == k_ceiling);
        plexus::inproc::inproc_bus<> ibus;
        plexus::inproc::inproc_executor<> iex{ibus};
        plexus::inproc::inproc_channel<> unprobed{iex};
        REQUIRE(pio::detail::channel_frame_ceiling(unprobed) == (std::numeric_limits<std::size_t>::max)());
        REQUIRE(pio::detail::channel_frame_ceiling(relay.msg_peer().channel) == k_ceiling);

        const bool attached = relay_messages.attach_for_fanout(serial_msg_fwd::peer{relay.msg_peer().channel, "narrow"}, "gated");
        pump_until(io, [&] { return true; });
        REQUIRE(attached);

        pio::forward_options opts;
        opts.splice_slot_bytes = 16u * 1024; // room for the oversized envelope in a pool slot
        splice sp{pio::make_forward_ctx(opts)};
        const auto hash = wire::fqn_topic_hash("gated");
        const auto origin = id_of(0xB0);

        // An OVERSIZED forwarded frame toward the narrow leg: gated at the splice, never sent.
        const auto big_inner = capture_inner("gated", 6u * 1024);
        REQUIRE(big_inner.size() > k_ceiling);
        sp.refan(relay_messages, hash, origin, /*hop=*/1, big_inner, /*arrival=*/nullptr, nullptr);
        REQUIRE(sp.exhaustion_drops() == 0u);
        pump_until(io, [&] { return oversize_drops > 0; });

        REQUIRE(oversize_drops == 1u);  // drop-with-count at the splice
        REQUIRE(peer_deliveries == 0u); // the oversized frame never crossed
        REQUIRE(peer.is_complete());    // and the narrow session was NOT torn down

        // A fitting frame on the SAME leg transits and delivers — the drop above was size-selective
        // over a live wire, not a dead one.
        const auto small_inner = capture_inner("gated", 16);
        sp.refan(relay_messages, hash, origin, /*hop=*/2, small_inner, /*arrival=*/nullptr, nullptr);
        pump_until(io, [&] { return peer_deliveries > 0; });

        REQUIRE(peer_deliveries == 1u);
        REQUIRE(peer.is_complete());
        REQUIRE(oversize_drops == 1u); // the fitting frame added no new drop
    }
}

namespace {

using ms = std::chrono::milliseconds;

std::vector<std::byte> make_payload(std::size_t n)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + (i >> 8)) & 0xFFu);
    return out;
}

template<typename Pred>
void pump(::asio::io_context &io, Pred pred, ms timeout = ms{15000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

}

TEST_CASE("envelope gate: a multi-fragment UDP message reassembles on the wide inbound leg and refragments "
          "onto a narrower outbound leg, byte-for-byte at the consumer",
          "[integration][forward][envelope][udp][refragment]")
{
    constexpr int k_runs             = 3;
    constexpr std::size_t budget_in  = 8192; // wide inbound fragment budget
    constexpr std::size_t budget_out = 1200; // narrower outbound budget: forces refragmentation
    constexpr std::size_t payload_sz = 32u * 1024;

    const pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};
    const auto arq = plexus::datagram::detail::udp_arq_config{.window = 1024, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{160}, .max_retransmit = 40};

    int proven = 0;
    for(int run = 0; run < k_runs; ++run)
    {
        ::asio::io_context io;
        pasio::udp_transport consumer{io, budget_out, pasio::udp_transport::arq_type::default_ladder, arq};
        pasio::udp_transport relay_out{io, budget_out, fast_hs, arq};
        pasio::udp_transport relay_in{io, budget_in, pasio::udp_transport::arq_type::default_ladder, arq};
        pasio::udp_transport origin{io, budget_in, fast_hs, arq};

        std::unique_ptr<pasio::udp_channel> consumer_ch, relay_out_ch, relay_in_ch, origin_ch;
        std::vector<std::byte> delivered;

        consumer.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    consumer_ch = std::move(ch);
                    consumer_ch->on_data([&](std::span<const std::byte> b) { delivered.assign(b.begin(), b.end()); });
                });
        consumer.listen({"udp", "127.0.0.1:0"});
        pump(io, [&] { return consumer.port() != 0; });

        // The relay reassembles a whole inbound message and re-sends it on the narrower outbound leg.
        relay_in.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    relay_in_ch = std::move(ch);
                    relay_in_ch->on_data([&](std::span<const std::byte> whole) { if(relay_out_ch) relay_out_ch->send(whole); });
                });
        relay_in.listen({"udp", "127.0.0.1:0"});
        pump(io, [&] { return relay_in.port() != 0; });

        relay_out.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { relay_out_ch = std::move(ch); });
        relay_out.dial({"udpr", "127.0.0.1:" + std::to_string(consumer.port())});
        origin.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { origin_ch = std::move(ch); });
        origin.dial({"udpr", "127.0.0.1:" + std::to_string(relay_in.port())});
        pump(io, [&] { return relay_out_ch && origin_ch && relay_in_ch; });
        REQUIRE(relay_out_ch != nullptr);
        REQUIRE(origin_ch != nullptr);

        const auto payload = make_payload(payload_sz);
        REQUIRE(payload.size() > budget_in);  // genuinely multi-fragment inbound
        REQUIRE(payload.size() > budget_out); // genuinely multi-fragment (refragmented) outbound
        origin_ch->send(payload);

        pump(io, [&] { return !delivered.empty(); });
        REQUIRE(delivered.size() == payload.size());
        REQUIRE(std::equal(delivered.begin(), delivered.end(), payload.begin())); // byte-for-byte through the relay
        ++proven;
    }
    REQUIRE(proven == k_runs);
}
