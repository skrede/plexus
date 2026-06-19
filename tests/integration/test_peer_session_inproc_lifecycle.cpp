#include "test_peer_session_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace peer_session_inproc_fixture;

namespace {

struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point        now() noexcept { return current; }
    static void              reset() noexcept { current = time_point{}; }
    static void              advance(duration d) noexcept { current += d; }
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
    inproc_bus<manual_clock>      bus;
    inproc_executor<manual_clock> ex{bus};
    inproc_channel<manual_clock>  peer_ch{ex}; // a silent peer that never responds

    manual_msg messages{};
    manual_rpc procedures{ex, std::chrono::hours(1)};

    plexus::io::peer_context<manual_policy> ctx; // the record owns the dialer channel
    manual_session                          requester;

    explicit timeout_harness(std::chrono::nanoseconds timeout)
            : ctx{std::make_unique<inproc_channel<manual_clock>>(ex), {}, "silent-node", {}, {}}
            , requester(ctx, ex, make_cfg(0x02), timeout, messages, procedures, false)
    {
        ctx.channel->connect_to(
                peer_ch.local_endpoint()); // sends land on a peer that never replies
        requester.start();
    }

    void drive() { ex.drain(); }
};

}

TEST_CASE("inproc peer_session: a handshake that never completes aborts once the timeout passes "
          "(virtual clock), looped",
          "[integration][peer_session][inproc]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        manual_clock::reset();
        const auto      deadline = std::chrono::milliseconds(50);
        timeout_harness h(deadline);

        // Before the deadline: the request was sent but no response arrived — no abort.
        h.drive();
        REQUIRE(!h.requester.is_complete());

        // Cross the deadline and drive: on_timeout fires abort -> tear_down.
        manual_clock::advance(deadline + std::chrono::milliseconds(1));
        h.drive();
        REQUIRE(!h.requester.is_complete());
        REQUIRE(h.requester.peer_session_id() == 0); // epoch latch reset by teardown
    }
}

namespace {

// A manual-clock pair stood up through the manual-clock transport rendezvous (no
// hand-dial) so the handshake CAN complete before the deadline; used to prove the
// timer is cancelled on completion (no later abort fires).
struct manual_link
{
    inproc_bus<manual_clock>       bus;
    inproc_executor<manual_clock>  ex{bus};
    inproc_transport<manual_clock> transport{ex, bus};

    manual_msg req_messages{};
    manual_msg resp_messages{};
    manual_rpc req_procedures{ex, std::chrono::hours(1)};
    manual_rpc resp_procedures{ex, std::chrono::hours(1)};

    plexus::io::peer_context<manual_policy> req_ctx;  // the dialer slot's per-peer record
    plexus::io::peer_context<manual_policy> resp_ctx; // the accepted slot's per-peer record
    std::optional<manual_session>           requester;
    std::optional<manual_session>           responder;

    explicit manual_link(std::chrono::nanoseconds timeout)
    {
        transport.on_accepted(
                [this, timeout](std::unique_ptr<inproc_channel<manual_clock>> ch)
                {
                    resp_ctx.channel   = std::move(ch);
                    resp_ctx.node_name = "requester-node";
                    responder.emplace(resp_ctx, ex, make_cfg(0x01), timeout, resp_messages,
                                      resp_procedures, true);
                    responder->start();
                });
        transport.on_dialed(
                [this, timeout](std::unique_ptr<inproc_channel<manual_clock>> ch,
                                const plexus::io::endpoint &)
                {
                    req_ctx.channel   = std::move(ch);
                    req_ctx.node_name = "responder-node";
                    requester.emplace(req_ctx, ex, make_cfg(0x02), timeout, req_messages,
                                      req_procedures, false);
                    requester->start();
                });
        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive() { ex.drain(); }
};

}

TEST_CASE("inproc peer_session: completing before the deadline cancels the timer — no later abort, "
          "looped",
          "[integration][peer_session][inproc]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        manual_clock::reset();
        const auto  deadline = std::chrono::milliseconds(50);
        manual_link l(deadline);

        l.drive();
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());

        // Advance well past the (now-cancelled) deadline: NO abort fires; the
        // session stays complete with its minted epoch intact.
        const auto epoch = l.requester->session_id();
        manual_clock::advance(deadline + std::chrono::seconds(10));
        l.drive();
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.requester->session_id() == epoch);
    }
}

TEST_CASE("inproc peer_session: teardown drains the forwarders and resets the epoch latch",
          "[integration][peer_session][inproc]")
{
    link l;
    l.drive();
    REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
    l.drive();

    // Latch the responder's view of the requester's epoch with a real publish.
    l.req_messages.publish("topic", as_bytes(std::string{"x"}), l.requester->session_id());
    l.drive();
    REQUIRE(l.responder->peer_session_id() == l.requester->session_id());

    l.responder->tear_down();
    REQUIRE(!l.responder->is_complete());
    REQUIRE(l.responder->peer_session_id() == 0);

    // After teardown the responder no longer fans toward its peer: a publish from
    // the responder reaches nobody (detach_all dropped the fan-out entry).
    l.resp_messages.attach_for_fanout(l.responder->msg_peer(), "back");
    l.resp_messages.detach_all(l.responder->msg_peer());
    l.resp_messages.publish("back", as_bytes(std::string{"y"}), l.responder->session_id());
    l.drive();
    REQUIRE(l.req_received.empty());
}
