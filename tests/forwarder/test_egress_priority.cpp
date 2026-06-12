#include "plexus/io/message_forwarder.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/egress_scheduler.h"
#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/io/priority.h"

#include "plexus/policy.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <system_error>

using namespace plexus;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// Decode the opaque body of a UNIDIRECTIONAL data frame; returns "" for a control
// frame (subscribe_response etc.) so the send-order capture counts only data.
std::string data_body(const std::vector<std::byte> &f)
{
    auto hdr = wire::decode_header(f);
    if(!hdr || hdr->type != wire::msg_type::unidirectional)
        return {};
    auto inner = std::span<const std::byte>{f}.subspan(wire::header_size);
    auto decoded = wire::decode_unidirectional(inner);
    if(!decoded)
        return {};
    return std::string{reinterpret_cast<const char *>(decoded->data.data()), decoded->data.size()};
}

// A deterministic test substrate: a byte_channel that EXPOSES a controllable
// backpressured() stall and records each send's frame bytes in order. A test-owned
// `stall` member holds the reported occupancy >= k_low_water until released, so the
// scheduler bands instead of draining; the synchronous post (below) makes every drain
// run inline, so the ordering is deterministic without a real runtime.
struct stall_state
{
    std::size_t reported{0};                       // what backpressured() returns
    std::vector<std::vector<std::byte>> sends;     // every frame the channel was handed, in order
};

struct stall_executor
{
};

struct stall_channel
{
    explicit stall_channel(stall_state &st) : m_st(&st) {}
    stall_channel(stall_executor &) {}             // Policy convenience ctor (unused)
    stall_channel(stall_executor &, std::error_code &) {}

    void send(std::span<const std::byte> d) { m_st->sends.emplace_back(d.begin(), d.end()); }
    void close() {}
    [[nodiscard]] io::endpoint remote_endpoint() const { return {"tcp", "127.0.0.1:0"}; }
    void on_data(detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(detail::move_only_function<void()>) {}
    void on_error(detail::move_only_function<void(io::io_error)>) {}
    void on_protocol_close(detail::move_only_function<void(wire::close_cause)>) {}
    [[nodiscard]] std::size_t backpressured() const noexcept { return m_st->reported; }

    stall_state *m_st{nullptr};
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
    using executor_type = stall_executor &;
    using byte_channel_type = stall_channel;
    using timer_type = stall_timer;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type, detail::move_only_function<void()> fn) { fn(); }   // synchronous
};

static_assert(plexus::Policy<stall_policy>);

using stall_forwarder = io::message_forwarder<stall_policy>;

}

TEST_CASE("egress_priority: a realtime frame leaves a stalled destination before a background flood, looped",
          "[egress_priority][forwarder]")
{
    constexpr int k_iterations = 100;
    constexpr int k_flood = 50;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        stall_state st;
        stall_channel ch{st};
        stall_executor ex;
        stall_forwarder fwd{ex};

        // Two topics over the SAME destination, one background and one realtime.
        fwd.declare("bg", topic_qos{.priority = io::priority::background});
        fwd.declare("rt", topic_qos{.priority = io::priority::realtime});
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "bg"));
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "rt"));
        st.sends.clear();   // drop the subscribe_response control frames

        // Stall the destination so every publish bands but nothing drains.
        st.reported = io::detail::k_low_water + 1;
        for(int i = 0; i < k_flood; ++i)
            fwd.publish("bg", as_bytes("bg" + std::to_string(i)));
        fwd.publish("rt", as_bytes(std::string{"REALTIME"}));   // enqueued AFTER the whole flood
        REQUIRE(st.sends.empty());   // nothing left the stalled destination yet

        // Release the stall and kick a drain with one more background publish; the band
        // backlog drains highest-first, so REALTIME must surface before any bg frame.
        st.reported = 0;
        fwd.publish("bg", as_bytes(std::string{"bg-trigger"}));

        // The realtime body must appear in the send order before ANY background body.
        std::size_t rt_index = st.sends.size();
        std::size_t first_bg_index = st.sends.size();
        for(std::size_t i = 0; i < st.sends.size(); ++i)
        {
            const std::string body = data_body(st.sends[i]);
            if(body == "REALTIME" && rt_index == st.sends.size())
                rt_index = i;
            if(body.rfind("bg", 0) == 0 && first_bg_index == st.sends.size())
                first_bg_index = i;
        }
        REQUIRE(rt_index < st.sends.size());          // the realtime frame did leave
        REQUIRE(first_bg_index < st.sends.size());    // background frames left too
        REQUIRE(rt_index < first_bg_index);           // realtime strictly before any background
    }
}

TEST_CASE("egress_priority: a flooded background band never delays a high frame (no starvation of high), looped",
          "[egress_priority][forwarder]")
{
    constexpr int k_iterations = 100;
    constexpr int k_flood = 40;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        stall_state st;
        stall_channel ch{st};
        stall_executor ex;
        stall_forwarder fwd{ex};

        fwd.declare("bg", topic_qos{.priority = io::priority::background});
        fwd.declare("hi", topic_qos{.priority = io::priority::high});
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "bg"));
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "hi"));
        st.sends.clear();

        // Build a deep background backlog under a stall, then publish ONE high frame
        // mid-flood — also under the stall, so it joins the high band behind nothing.
        st.reported = io::detail::k_low_water + 1;
        for(int i = 0; i < k_flood; ++i)
            fwd.publish("bg", as_bytes("bg" + std::to_string(i)));
        fwd.publish("hi", as_bytes(std::string{"HIGH"}));
        for(int i = k_flood; i < 2 * k_flood; ++i)
            fwd.publish("bg", as_bytes("bg" + std::to_string(i)));
        REQUIRE(st.sends.empty());

        // Release and drain: the high band drains fully before the background band, so
        // the single high frame is the FIRST data frame out, ahead of the whole backlog.
        st.reported = 0;
        fwd.publish("bg", as_bytes(std::string{"bg-trigger"}));

        // The first data frame out must be HIGH (it never queued behind the background flood).
        std::string first_body;
        for(const auto &f : st.sends)
        {
            const std::string body = data_body(f);
            if(!body.empty()) { first_body = body; break; }
        }
        REQUIRE(first_body == "HIGH");
    }
}

TEST_CASE("egress_priority: inproc is unaffected — mixed priorities deliver in publish order, synchronously",
          "[egress_priority][forwarder]")
{
    using inproc_forwarder = io::message_forwarder<inproc::inproc_policy>;

    // The inproc capture substrate: a second channel records every delivered frame.
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc::inproc_bus<> bus;
        inproc::inproc_executor<> ex(bus);
        inproc::inproc_channel<> ch(ex);
        inproc::inproc_channel<> sink(ex);
        ch.connect_to(sink.local_endpoint());
        std::vector<std::vector<std::byte>> frames;
        sink.on_data([&](std::span<const std::byte> d) { frames.emplace_back(d.begin(), d.end()); });

        inproc_forwarder fwd{ex};
        fwd.declare("bg", topic_qos{.priority = io::priority::background});
        fwd.declare("rt", topic_qos{.priority = io::priority::realtime});
        REQUIRE(fwd.attach_for_fanout(inproc_forwarder::peer{ch, "node-a"}, "bg"));
        REQUIRE(fwd.attach_for_fanout(inproc_forwarder::peer{ch, "node-a"}, "rt"));
        ex.drain();
        frames.clear();

        // inproc has no backpressured(): the scheduler short-circuits to direct
        // synchronous send, so a background-then-realtime publish keeps PUBLISH order
        // (no banding, no reorder) — byte-identical to the pre-scheduler path.
        fwd.publish("bg", as_bytes(std::string{"first-bg"}));
        fwd.publish("rt", as_bytes(std::string{"second-rt"}));
        ex.drain();

        std::vector<std::string> bodies;
        for(const auto &f : frames)
        {
            const std::string b = data_body(f);
            if(!b.empty())
                bodies.emplace_back(b);
        }
        REQUIRE(bodies == std::vector<std::string>{"first-bg", "second-rt"});   // publish order, no reorder
    }
}

TEST_CASE("egress_priority: the MUX/ERASED path bands — realtime leaves before a background flood over a polymorphic_byte_channel, looped",
          "[egress_priority][forwarder]")
{
    // Amendment-1 enforcement: prove QOS-05 holds on the flagship type-erased path. The
    // egress scheduler reads backpressured() THROUGH the erasure (channel_adapter ->
    // concrete), so a polymorphic_byte_channel wrapping a stalled concrete channel bands
    // exactly like the bare concrete one — not the always-accept short-circuit.
    namespace pio = plexus::io;

    // A Policy whose byte_channel is the type-erased polymorphic_byte_channel, mirroring
    // the production mux config; the erased member is a stall_channel exposing the
    // controllable backpressured() the test drives.
    struct erased_policy
    {
        using executor_type = stall_executor &;
        using byte_channel_type = pio::polymorphic_byte_channel;
        using timer_type = stall_timer;
        using byte_owner = std::shared_ptr<const void>;
        static void post(executor_type, detail::move_only_function<void()> fn) { fn(); }
    };
    static_assert(plexus::Policy<erased_policy>);
    using erased_forwarder = io::message_forwarder<erased_policy>;

    // Sanity: the erasure exposes backpressured(), so the scheduler's capability gate is
    // true and the erased path bands rather than always-accepting.
    static_assert(requires(pio::polymorphic_byte_channel &c) { c.backpressured(); },
                  "polymorphic_byte_channel must expose backpressured() so the mux path bands");

    constexpr int k_iterations = 100;
    constexpr int k_flood = 50;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        stall_state st;
        auto inner = std::make_unique<stall_channel>(st);
        pio::polymorphic_byte_channel ch(
            std::make_unique<pio::channel_adapter<stall_channel>>(std::move(inner)));
        stall_executor ex;
        erased_forwarder fwd{ex};

        fwd.declare("bg", topic_qos{.priority = io::priority::background});
        fwd.declare("rt", topic_qos{.priority = io::priority::realtime});
        REQUIRE(fwd.attach_for_fanout(erased_forwarder::peer{ch, "node-a"}, "bg"));
        REQUIRE(fwd.attach_for_fanout(erased_forwarder::peer{ch, "node-a"}, "rt"));
        st.sends.clear();

        st.reported = io::detail::k_low_water + 1;
        for(int i = 0; i < k_flood; ++i)
            fwd.publish("bg", as_bytes("bg" + std::to_string(i)));
        fwd.publish("rt", as_bytes(std::string{"REALTIME"}));
        REQUIRE(st.sends.empty());   // the ERASED path banded (no always-accept short-circuit)

        st.reported = 0;
        fwd.publish("bg", as_bytes(std::string{"bg-trigger"}));

        std::size_t rt_index = st.sends.size();
        std::size_t first_bg_index = st.sends.size();
        for(std::size_t i = 0; i < st.sends.size(); ++i)
        {
            const std::string body = data_body(st.sends[i]);
            if(body == "REALTIME" && rt_index == st.sends.size())
                rt_index = i;
            if(body.rfind("bg", 0) == 0 && first_bg_index == st.sends.size())
                first_bg_index = i;
        }
        REQUIRE(rt_index < st.sends.size());
        REQUIRE(first_bg_index < st.sends.size());
        REQUIRE(rt_index < first_bg_index);   // realtime strictly before background, over the erasure
    }
}

namespace {

// Decode every data body in the send-order capture, skipping control frames.
std::vector<std::string> data_bodies(const std::vector<std::vector<std::byte>> &sends)
{
    std::vector<std::string> out;
    for(const auto &f : sends)
    {
        const std::string b = data_body(f);
        if(!b.empty())
            out.emplace_back(b);
    }
    return out;
}

// Feed a directly-driven scheduler k_band_depth + 1 enqueues at the normal band under
// one congestion mode while the destination reports a stall, so the band saturates and
// the (k_band_depth+1)-th enqueue takes the at-capacity branch. Returns the band index.
std::size_t saturate_scheduler(io::detail::egress_scheduler<stall_channel, stall_policy> &sched,
                               stall_channel &ch, stall_state &st, io::congestion mode)
{
    st.reported = io::detail::k_low_water + 1;   // stalled: nothing drains, the band fills
    const std::size_t band = io::detail::band_of(io::priority::normal);
    for(std::size_t i = 0; i < io::detail::k_band_depth; ++i)
        sched.enqueue(ch, band, mode, as_bytes("f" + std::to_string(i)));
    sched.enqueue(ch, band, mode, as_bytes(std::string{"OVERFLOW"}));
    return band;
}

}

TEST_CASE("egress_priority: drop_oldest evicts the oldest resident frame; survivors arrive in FIFO order, looped",
          "[egress_priority][forwarder]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // The directly-driven scheduler proves the per-band counter (the observability gate).
        stall_state counter_st;
        stall_channel counter_ch{counter_st};
        stall_executor counter_ex;
        io::detail::egress_scheduler<stall_channel, stall_policy> sched{counter_ex};
        const std::size_t band = saturate_scheduler(sched, counter_ch, counter_st, io::congestion::drop_oldest);
        REQUIRE(sched.dropped_oldest(counter_ch, band) == 1);

        // The forwarder path proves the runtime wire EFFECT: the evicted oldest body is
        // absent, the new body present, the survivors in FIFO order.
        stall_state st;
        stall_channel ch{st};
        stall_executor ex;
        stall_forwarder fwd{ex};
        // The saturated topic is on the normal band; the drain trigger rides a separate
        // realtime topic on an EMPTY higher band, so the trigger never re-saturates the
        // normal band — it only releases the stall and kicks the inline drain.
        fwd.declare("t", topic_qos{.congestion = io::congestion::drop_oldest, .priority = io::priority::normal});
        fwd.declare("kick", topic_qos{.priority = io::priority::realtime});
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "t"));
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "kick"));
        st.sends.clear();

        st.reported = io::detail::k_low_water + 1;   // stall: every publish bands, nothing drains
        for(std::size_t i = 0; i < io::detail::k_band_depth; ++i)
            fwd.publish("t", as_bytes("f" + std::to_string(i)));
        fwd.publish("t", as_bytes(std::string{"OVERFLOW"}));   // evicts f0
        REQUIRE(st.sends.empty());

        st.reported = 0;
        fwd.publish("kick", as_bytes(std::string{"KICK"}));   // releases the stall and drains

        // Drain order is highest band first: the realtime KICK leaves before the normal
        // band survivors, then f1..f{N-1}, OVERFLOW in FIFO order (f0 evicted, absent).
        std::vector<std::string> expected{"KICK"};
        for(std::size_t i = 1; i < io::detail::k_band_depth; ++i)
            expected.emplace_back("f" + std::to_string(i));
        expected.emplace_back("OVERFLOW");

        const auto bodies = data_bodies(st.sends);
        REQUIRE(bodies == expected);   // f0 evicted (absent), OVERFLOW present, FIFO order
    }
}

TEST_CASE("egress_priority: block refuses the new frame at a saturated band; it never reaches the wire, looped",
          "[egress_priority][forwarder]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        stall_state counter_st;
        stall_channel counter_ch{counter_st};
        stall_executor counter_ex;
        io::detail::egress_scheduler<stall_channel, stall_policy> sched{counter_ex};
        const std::size_t band = saturate_scheduler(sched, counter_ch, counter_st, io::congestion::block);
        REQUIRE(sched.blocked(counter_ch, band) == 1);

        stall_state st;
        stall_channel ch{st};
        stall_executor ex;
        stall_forwarder fwd{ex};
        fwd.declare("t", topic_qos{.congestion = io::congestion::block, .priority = io::priority::normal});
        fwd.declare("kick", topic_qos{.priority = io::priority::realtime});
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "t"));
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "kick"));
        st.sends.clear();

        st.reported = io::detail::k_low_water + 1;
        for(std::size_t i = 0; i < io::detail::k_band_depth; ++i)
            fwd.publish("t", as_bytes("f" + std::to_string(i)));
        fwd.publish("t", as_bytes(std::string{"REFUSED"}));   // refused, never admitted
        REQUIRE(st.sends.empty());

        st.reported = 0;
        fwd.publish("kick", as_bytes(std::string{"KICK"}));   // releases the stall and drains

        std::vector<std::string> expected{"KICK"};
        for(std::size_t i = 0; i < io::detail::k_band_depth; ++i)
            expected.emplace_back("f" + std::to_string(i));

        const auto bodies = data_bodies(st.sends);
        REQUIRE(bodies == expected);   // the original window survives, REFUSED never sent
    }
}

TEST_CASE("egress_priority: a forced drop is observable per-topic-per-band on the forwarder accessor",
          "[egress_priority][forwarder]")
{
    // The per-(topic, band) drop counter on the topic record is bumped at the fan-out
    // site from the band's overflow verdict, and read on demand through the forwarder
    // accessor. Each cause increments its own tally for the topic's own band; an untouched
    // (topic, band, cause) reads 0.
    namespace pdetail = io::detail;
    auto force_drops = [](io::congestion mode, pdetail::drop_cause cause, std::size_t overflows) {
        stall_state st;
        stall_channel ch{st};
        stall_executor ex;
        stall_forwarder fwd{ex};
        fwd.declare("t", topic_qos{.congestion = mode, .priority = io::priority::normal});
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "t"));

        const std::size_t band = io::detail::band_of(io::priority::normal);
        st.reported = io::detail::k_low_water + 1;            // stalled: the band fills, then overflows
        for(std::size_t i = 0; i < io::detail::k_band_depth; ++i)
            fwd.publish("t", as_bytes("f" + std::to_string(i)));
        REQUIRE(fwd.dropped("t", band, cause) == 0);         // no overflow yet
        for(std::size_t i = 0; i < overflows; ++i)
            fwd.publish("t", as_bytes("over" + std::to_string(i)));

        REQUIRE(fwd.dropped("t", band, cause) == overflows); // every overflow counted under its cause
        // The other causes for the same (topic, band) stay zero — the verdict is exact.
        for(auto other : {pdetail::drop_cause::drop_oldest, pdetail::drop_cause::drop_newest,
                          pdetail::drop_cause::blocked})
            if(other != cause)
                REQUIRE(fwd.dropped("t", band, other) == 0);
        // A different band for the same topic is untouched.
        REQUIRE(fwd.dropped("t", io::detail::band_of(io::priority::realtime), cause) == 0);
    };

    force_drops(io::congestion::drop_oldest, pdetail::drop_cause::drop_oldest, 3);
    force_drops(io::congestion::drop_newest, pdetail::drop_cause::drop_newest, 5);
    force_drops(io::congestion::block,       pdetail::drop_cause::blocked,     4);
}

TEST_CASE("egress_priority: drop_newest refuses the new frame at a saturated band; it never reaches the wire, looped",
          "[egress_priority][forwarder]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        stall_state counter_st;
        stall_channel counter_ch{counter_st};
        stall_executor counter_ex;
        io::detail::egress_scheduler<stall_channel, stall_policy> sched{counter_ex};
        const std::size_t band = saturate_scheduler(sched, counter_ch, counter_st, io::congestion::drop_newest);
        REQUIRE(sched.dropped_newest(counter_ch, band) == 1);

        stall_state st;
        stall_channel ch{st};
        stall_executor ex;
        stall_forwarder fwd{ex};
        fwd.declare("t", topic_qos{.congestion = io::congestion::drop_newest, .priority = io::priority::normal});
        fwd.declare("kick", topic_qos{.priority = io::priority::realtime});
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "t"));
        REQUIRE(fwd.attach_for_fanout(stall_forwarder::peer{ch, "node-a"}, "kick"));
        st.sends.clear();

        st.reported = io::detail::k_low_water + 1;
        for(std::size_t i = 0; i < io::detail::k_band_depth; ++i)
            fwd.publish("t", as_bytes("f" + std::to_string(i)));
        fwd.publish("t", as_bytes(std::string{"REFUSED"}));   // refused, never admitted
        REQUIRE(st.sends.empty());

        st.reported = 0;
        fwd.publish("kick", as_bytes(std::string{"KICK"}));   // releases the stall and drains

        std::vector<std::string> expected{"KICK"};
        for(std::size_t i = 0; i < io::detail::k_band_depth; ++i)
            expected.emplace_back("f" + std::to_string(i));

        const auto bodies = data_bodies(st.sends);
        REQUIRE(bodies == expected);   // the original window survives, REFUSED never sent
    }
}
