// The bounded-egress proof for the relay splice: two destinations subscribe one topic, one sink is
// backpressured to saturation while the other drains freely. The splice fans each forwarded frame to
// both through the SAME per-destination egress scheduler, so the saturated leg drops-with-count in its
// own band while the fast leg keeps delivering — no head-of-line blocking — and the delivering leg's
// sequence stays monotone (per-(origin, band) FIFO). Cross-band order is intentionally NOT asserted.

#include "plexus/io/message_forwarder.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/forward_splice.h"
#include "plexus/io/detail/priority_band_queue.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/priority.h"
#include "plexus/io/congestion.h"
#include "plexus/io/null_logger.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/forwarded_frame.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <system_error>

using namespace plexus;
namespace wire = plexus::wire;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

node_id id_of(std::uint8_t base)
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(base + i);
    return id;
}

// A header-on unidirectional frame carrying a distinct body, captured off a producer's inproc wire.
std::vector<std::byte> capture_inner(std::string_view fqn, std::string_view body)
{
    inproc::inproc_bus<> bus;
    inproc::inproc_executor<> ex(bus);
    inproc::inproc_channel<> sub(ex);
    inproc::inproc_channel<> cap(ex);
    sub.connect_to(cap.local_endpoint());
    std::vector<std::byte> framed;
    cap.on_data([&](std::span<const std::byte> f) { framed.assign(f.begin(), f.end()); });

    log::null_logger sink;
    io::message_forwarder<inproc::inproc_policy> producer{sink};
    producer.declare(fqn, topic_qos{});
    producer.attach_for_fanout(io::message_forwarder<inproc::inproc_policy>::peer{sub, "producer"}, fqn);
    ex.drain();
    producer.publish(fqn, as_bytes(std::string{body}));
    ex.drain();
    return framed;
}

// A byte_channel whose backpressure occupancy the test controls and that records every admitted frame,
// mirroring the egress-priority stall substrate: a synchronous post makes the scheduler drain inline.
struct stall_state
{
    std::size_t reported{0};
    std::vector<std::vector<std::byte>> sends;
};

inline std::uint64_t next_stall_key() noexcept
{
    static std::uint64_t counter = 0;
    return ++counter;
}

struct stall_channel
{
    explicit stall_channel(stall_state &st) : m_st(&st) {}
    void send(std::span<const std::byte> d) { m_st->sends.emplace_back(d.begin(), d.end()); }
    void close() {}
    io::endpoint remote_endpoint() const { return {"tcp", "127.0.0.1:0"}; }
    void on_data(detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(detail::move_only_function<void()>) {}
    void on_error(detail::move_only_function<void(io::io_error)>) {}
    void on_protocol_close(detail::move_only_function<void(wire::close_cause)>) {}
    std::size_t backpressured() const noexcept { return m_st->reported; }
    std::uint64_t scheduler_key() const noexcept { return m_key; }

    stall_state *m_st{nullptr};
    std::uint64_t m_key{next_stall_key()};
};

struct stall_executor
{
};

struct stall_timer
{
    explicit stall_timer(stall_executor &) {}
    stall_timer(stall_executor &, std::error_code &) {}
    void expires_after(std::chrono::milliseconds) {}
    void async_wait(detail::move_only_function<void(std::error_code)>) {}
    void cancel() {}
};

struct stall_policy
{
    using executor_type     = stall_executor &;
    using byte_channel_type = stall_channel;
    using timer_type        = stall_timer;
    using byte_owner        = std::shared_ptr<const void>;
    static void post(executor_type, detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<stall_policy>);

using forwarder = io::message_forwarder<stall_policy>;
using fwd_splice = io::forward_splice<stall_policy>;

// A pool sized ABOVE the band depth so the saturated band (not the pool) is the drop site under test.
io::forward_ctx pooled_ctx(std::size_t slots)
{
    io::forward_options opts;
    opts.splice_pool_slots = slots;
    return io::make_forward_ctx(opts);
}

// The inner payload a captured forwarded frame carried (the header-on frame the splice fanned verbatim).
std::vector<std::byte> inner_of(const std::vector<std::byte> &framed)
{
    auto hdr = wire::decode_header(framed);
    if(!hdr || hdr->type != wire::msg_type::forwarded)
        return {};
    auto ff = wire::decode_forwarded_frame(std::span<const std::byte>{framed}.subspan(wire::header_size));
    return ff ? ff->inner : std::vector<std::byte>{};
}

}

TEST_CASE("forward_splice_no_hol: a saturated destination drops-with-count while a second destination "
          "keeps delivering",
          "[integration][forward_splice][no-hol]")
{
    const std::size_t band   = io::detail::band_of(io::priority::normal);
    const std::size_t depth  = io::detail::k_band_depth;
    const std::size_t frames = depth + 5; // overflow the slow band by five

    stall_state fast_st;
    stall_state slow_st;
    stall_channel fast{fast_st};
    stall_channel slow{slow_st};

    log::null_logger sink;
    forwarder fwd{sink};
    fwd.declare("relayed.topic", topic_qos{.congestion = io::congestion::drop_oldest, .priority = io::priority::normal});
    REQUIRE(fwd.attach_for_fanout(forwarder::peer{fast, "fast"}, "relayed.topic"));
    REQUIRE(fwd.attach_for_fanout(forwarder::peer{slow, "slow"}, "relayed.topic"));
    fast_st.sends.clear(); // drop the subscribe_response control frames
    slow_st.sends.clear();
    slow_st.reported = io::detail::k_low_water + 1; // saturated: nothing drains, the band fills then drops
    fast_st.reported = 0;                           // free: every fanned frame drains immediately

    const auto inner = capture_inner("relayed.topic", "m");
    fwd_splice sp{pooled_ctx(depth + 16)};
    const node_id origin = id_of(0xA0);
    for(std::size_t i = 0; i < frames; ++i)
        sp.refan(fwd, wire::fqn_topic_hash("relayed.topic"), origin, /*hop=*/1, inner, /*arrival=*/nullptr, nullptr);

    // The saturated leg dropped-with-count in its own band: exactly the overflow beyond the band depth.
    // The pool was sized above the band, so the band's drop_oldest — not pool exhaustion — is the drop.
    REQUIRE(fwd.dropped("relayed.topic", band, io::detail::drop_cause::drop_oldest) == frames - depth);
    REQUIRE(fwd.dropped("relayed.topic", band, io::detail::drop_cause::drop_oldest) > 0u);
    REQUIRE(sp.exhaustion_drops() == 0u);

    // No head-of-line blocking: the fast leg delivered every frame regardless of the slow leg's backlog.
    REQUIRE(fast_st.sends.size() == frames);
}

TEST_CASE("forward_splice_no_hol: a slow leg never stalls the fast leg's drain; per-origin FIFO holds", "[integration][forward_splice][no-hol][fifo]")
{
    // Both legs stall so the fast leg builds a shallow banded backlog (well under the band depth, so the
    // band never drops); releasing ONLY the fast leg drains its backlog in strict publish order while the
    // slow leg stays saturated — the backlog is per-destination and per-(origin, band) FIFO is held.
    const std::size_t depth = 8;

    stall_state fast_st;
    stall_state slow_st;
    stall_channel fast{fast_st};
    stall_channel slow{slow_st};

    log::null_logger sink;
    forwarder fwd{sink};
    fwd.declare("relayed.topic", topic_qos{.congestion = io::congestion::drop_oldest, .priority = io::priority::normal});
    REQUIRE(fwd.attach_for_fanout(forwarder::peer{fast, "fast"}, "relayed.topic"));
    REQUIRE(fwd.attach_for_fanout(forwarder::peer{slow, "slow"}, "relayed.topic"));
    fast_st.sends.clear();
    slow_st.sends.clear();
    slow_st.reported = io::detail::k_low_water + 1; // saturated throughout
    fast_st.reported = io::detail::k_low_water + 1; // stalled: bands, then releases below

    std::vector<std::vector<std::byte>> inners;
    for(std::size_t i = 0; i < depth; ++i)
        inners.push_back(capture_inner("relayed.topic", "f" + std::to_string(i)));

    fwd_splice sp{pooled_ctx(depth + 4)};
    const node_id origin = id_of(0xA0);
    for(std::size_t i = 0; i < depth; ++i)
        sp.refan(fwd, wire::fqn_topic_hash("relayed.topic"), origin, 1, inners[i], nullptr, nullptr);
    REQUIRE(fast_st.sends.empty()); // both legs stalled: nothing drained yet

    fast_st.reported = 0; // release ONLY the fast leg; the slow leg stays saturated
    auto extra       = capture_inner("relayed.topic", "f-kick");
    sp.refan(fwd, wire::fqn_topic_hash("relayed.topic"), origin, 1, extra, nullptr, nullptr);

    REQUIRE(fast_st.sends.size() == depth + 1); // the whole fast backlog drained despite the slow leg
    for(std::size_t i = 0; i < depth; ++i)
        REQUIRE(inner_of(fast_st.sends[i]) == inners[i]); // publish order held (per-origin FIFO)
    REQUIRE(inner_of(fast_st.sends[depth]) == extra);
    // Cross-band ordering is intentionally NOT asserted — the reorder freedom is by design.
}
