#include "test_peer_session_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace peer_session_inproc_fixture;

namespace {

// Synthesize a unidirectional data frame for "topic" carrying a chosen session_id,
// exactly as the forwarder frames one — so handing it to a receiver's on_receive
// exercises the production staleness gate, not a hand-strip.
std::vector<std::byte> make_data_frame(const std::string &payload, std::uint64_t session_id)
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    plexus::log::null_logger sink;
    msg_forwarder framer{sink};
    inproc_channel<> capture(ex);
    inproc_channel<> tx(ex);
    tx.connect_to(capture.local_endpoint());
    std::vector<std::byte> captured;
    capture.on_data([&](std::span<const std::byte> f) { captured.assign(f.begin(), f.end()); });
    msg_forwarder::peer peer{tx, "x"};
    framer.attach_for_fanout(peer, "topic");
    ex.drain();
    framer.publish("topic", as_bytes(payload), session_id);
    ex.drain();
    return captured;
}

}

TEST_CASE("inproc peer_session: the data-path staleness gate FIRES - a mismatched epoch is "
          "dropped, the latched epoch delivered, looped",
          "[integration][peer_session][inproc]")
{
    constexpr int k_iterations = 100;
    const std::string good     = "latched-epoch-bytes";
    const std::string stale    = "stale-session-bytes";
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        session_link l;
        l.drive();
        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.drive();

        // A real publish latches the requester's epoch on the responder.
        l.req_messages.publish("topic", as_bytes(good), l.requester->session_id());
        l.drive();
        REQUIRE(l.resp_received.size() == 1);
        const auto latched = l.responder->peer_session_id();
        REQUIRE(latched == l.requester->session_id());

        // A frame for the SAME topic carrying a DIFFERENT non-zero epoch is dropped:
        // the sink does not grow.
        const std::uint8_t stale_epoch = static_cast<std::uint8_t>(latched == 200 ? 199 : 200);
        auto stale_frame               = make_data_frame(stale, stale_epoch);
        l.responder->on_receive(stale_frame);
        l.drive();
        REQUIRE(l.resp_received.size() == 1); // DROPPED, not delivered

        // A frame carrying the latched epoch IS delivered.
        auto fresh_frame = make_data_frame(good, latched);
        l.responder->on_receive(fresh_frame);
        l.drive();
        REQUIRE(l.resp_received.size() == 2);
        REQUIRE(l.resp_received[1] == good);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc peer_session: the staleness gate distinguishes epochs beyond the u8 range - the "
          "255-wrap collision is gone, looped",
          "[integration][peer_session][inproc]")
{
    // Two epochs that ALIAS under a u8 epoch well — 257 ≡ 1 (mod 256) — are distinct
    // u64 values. The gate must latch one and drop the other. Under the retired u8
    // width these collided, so a dead incarnation's straggler (epoch 1) was wrongly
    // accepted against a live epoch-257 session. The u64 width makes them distinct.
    constexpr int k_iterations             = 100;
    constexpr std::uint64_t latched_epoch  = 257; // the live session's epoch
    constexpr std::uint64_t aliasing_epoch = 1;   // would alias 257 under u8
    int proven                             = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        session_link l;
        l.drive();
        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.drive();

        // A framed data frame carrying epoch 257 latches the FULL u64 epoch — not 1.
        l.responder->on_receive(make_data_frame("first", latched_epoch));
        l.drive();
        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.responder->peer_session_id() == latched_epoch);

        // A straggler carrying the aliasing epoch (1) is a DIFFERENT session: dropped.
        l.responder->on_receive(make_data_frame("aliasing", aliasing_epoch));
        l.drive();
        REQUIRE(l.resp_received.size() == 1); // DROPPED — no u8 collision

        // The latched epoch still delivers.
        l.responder->on_receive(make_data_frame("same", latched_epoch));
        l.drive();
        REQUIRE(l.resp_received.size() == 2);
        REQUIRE(l.resp_received[1] == "same");
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

namespace {

struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept
    {
        return current;
    }
    static void reset() noexcept
    {
        current = time_point{};
    }
    static void advance(duration d) noexcept
    {
        current += d;
    }
};

struct manual_policy
{
    using executor_type     = inproc_executor<manual_clock> &;
    using byte_channel_type = inproc_channel<manual_clock>;
    using timer_type        = plexus::inproc::inproc_timer<manual_clock>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<manual_policy>);

using manual_session = plexus::io::peer_session<manual_policy>;
using manual_msg     = plexus::io::message_forwarder<manual_policy>;
using manual_rpc     = plexus::io::procedure_forwarder<manual_policy>;

// A lone requester on the manual clock with no responder ever answering: the
// handshake never completes, so the armed handshake timer is what resolves it.
struct timeout_harness
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    inproc_channel<manual_clock> peer_ch{ex}; // a silent peer that never responds

    plexus::log::null_logger sink;
    manual_msg messages{sink};
    manual_rpc procedures{ex, std::chrono::hours(1), sink};

    plexus::io::peer_context<manual_policy> ctx; // the record owns the dialer channel
    manual_session requester;

    explicit timeout_harness(std::chrono::nanoseconds timeout)
            : ctx{std::make_unique<inproc_channel<manual_clock>>(ex), {}, "silent-node", {}, {}}
            , requester(ctx, ex, make_cfg(0x02), timeout, messages, procedures, false, sink)
    {
        ctx.channel->connect_to(peer_ch.local_endpoint()); // sends land on a peer that never replies
        requester.start();
    }

    void drive()
    {
        ex.drain();
    }
};

}
