#include "test_procedure_forwarder_common.h"

#include "plexus/inproc/inproc_timer.h"

using namespace procedure_forwarder_fixture;

namespace {

// A manual clock: now() returns an advance-able point the test moves by hand, so a
// per-call deadline fires deterministically — no wall-clock sleep, no polling. The
// static point is the std Clock idiom (mirrors steady_clock::now()); each case
// resets it so the loop iterations and cases never contaminate each other.
struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point        now() noexcept
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

// The manual-clock inproc Policy: the same substrate as inproc_policy, but every
// piece (executor, channel, timer) is instantiated on manual_clock so the
// forwarder's per-call timer fires off the advance-able clock.
struct manual_policy
{
    using executor_type     = plexus::inproc::inproc_executor<manual_clock> &;
    using byte_channel_type = plexus::inproc::inproc_channel<manual_clock>;
    using timer_type        = plexus::inproc::inproc_timer<manual_clock>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<manual_policy>);

using manual_forwarder = plexus::io::procedure_forwarder<manual_policy>;

// A manual-clock RPC link mirroring rpc_link, but on manual_clock so a deadline is
// driven by advance() rather than a wall clock. The provider serves NOTHING by
// default (the "never replies" state the timeout resolves); a case that wants a
// reply serves its own handler.
struct manual_link
{
    plexus::inproc::inproc_bus<manual_clock>      bus;
    plexus::inproc::inproc_executor<manual_clock> ex{bus};

    plexus::inproc::inproc_channel<manual_clock> caller_tx{ex};
    plexus::inproc::inproc_channel<manual_clock> caller_rx{ex};
    plexus::inproc::inproc_channel<manual_clock> provider_tx{ex};
    plexus::inproc::inproc_channel<manual_clock> provider_rx{ex};

    plexus::log::null_logger sink;

    manual_forwarder caller;
    manual_forwarder provider{ex, std::chrono::hours(1), sink};

    plexus::io::frame_router caller_router{sink};
    plexus::io::frame_router provider_router{sink};

    manual_forwarder::peer caller_peer{caller_tx, "provider-node"};
    manual_forwarder::peer provider_peer{provider_tx, "caller-node"};

    explicit manual_link(std::chrono::nanoseconds caller_deadline)
            : caller(ex, caller_deadline, sink)
    {
        caller_tx.connect_to(provider_rx.local_endpoint());
        provider_tx.connect_to(caller_rx.local_endpoint());

        provider_router.on_rpc_request([this](std::span<const std::byte> inner) { provider.deliver_request(provider_peer, inner); });
        caller_router.on_rpc_response([this](std::span<const std::byte> inner) { caller.deliver_response(caller_peer, inner); });
        provider_rx.on_data([this](std::span<const std::byte> f) { provider_router.route(f); });
        caller_rx.on_data([this](std::span<const std::byte> f) { caller_router.route(f); });
    }

    void drive()
    {
        ex.drain();
    }
};

}

TEST_CASE("a call whose provider never replies fires exactly one timeout once the deadline passes", "[procedure][timeout]")
{
    // Deterministic by construction: advance the manual clock PAST the deadline with
    // the provider serving nothing, then drain — the per-call timer fires exactly one
    // on_response(timeout, {}) and the outstanding entry is gone (a second drain
    // produces no further callback). Looped so a flake surfaces as a count mismatch.
    for(int iter = 0; iter < 100; ++iter)
    {
        manual_clock::reset();
        const auto  deadline = std::chrono::milliseconds(50);
        manual_link link(deadline);

        // A provider that receives the request and is dispatched, but NEVER replies
        // (it drops the reply on the floor) — the genuine hang-forever state the
        // timeout resolves. Serving a no-op handler is faithful: an UNSERVED fqn
        // would instead bounce back a no_handler response and resolve the call early.
        link.provider.serve("svc", [](std::span<const std::byte>, manual_forwarder::reply_fn &) {});

        int         fired    = 0;
        rpc_status  got      = rpc_status::error;
        std::size_t ret_size = 999;
        link.caller.call(link.caller_peer, "svc", {},
                         [&](rpc_status s, std::span<const std::byte> ret)
                         {
                             ++fired;
                             got      = s;
                             ret_size = ret.size();
                         });

        // Before the deadline: draining delivers the request to a provider that
        // serves nothing, but the caller's timer has NOT expired — no callback yet.
        link.drive();
        REQUIRE(fired == 0);

        // Cross the deadline and drive: the timer fires exactly once with timeout.
        manual_clock::advance(deadline + std::chrono::milliseconds(1));
        link.drive();

        REQUIRE(fired == 1);
        REQUIRE(got == rpc_status::timeout);
        REQUIRE(ret_size == 0);

        // The entry is gone: a further advance + drive produces no second callback,
        // and a late synthetic response for that corr_id (1) finds nothing to match.
        manual_clock::advance(deadline);
        link.drive();
        REQUIRE(fired == 1);

        auto late = make_response_inner(1u, rpc_status::success, {});
        link.caller.deliver_response(link.caller_peer, late);
        REQUIRE(fired == 1);
    }
}

TEST_CASE("a call answered before the deadline fires success and no later timeout (cancel-on-match)", "[procedure][timeout]")
{
    // The negative: a provider that DOES reply before the deadline resolves the call
    // with success; advancing the clock past the (now-cancelled) deadline afterward
    // produces NO second (timeout) callback. Proves cancel-on-match disarms the timer.
    for(int iter = 0; iter < 100; ++iter)
    {
        manual_clock::reset();
        const auto  deadline = std::chrono::milliseconds(50);
        manual_link link(deadline);

        link.provider.serve("svc", [](std::span<const std::byte> param, manual_forwarder::reply_fn &reply) { reply(rpc_status::success, param); });

        int               fired = 0;
        rpc_status        got   = rpc_status::error;
        std::string       ret;
        const std::string param = "answered";
        link.caller.call(link.caller_peer, "svc", as_bytes(param),
                         [&](rpc_status s, std::span<const std::byte> r)
                         {
                             ++fired;
                             got = s;
                             ret = to_string(r);
                         });

        // Resolve the call WITHOUT advancing the clock — the response arrives before
        // the deadline, so the match cancels the timer.
        link.drive();
        REQUIRE(fired == 1);
        REQUIRE(got == rpc_status::success);
        REQUIRE(ret == param);

        // Now cross the deadline: the cancelled timer must NOT fire a second time.
        manual_clock::advance(deadline + std::chrono::milliseconds(1));
        link.drive();
        REQUIRE(fired == 1);
        REQUIRE(got == rpc_status::success);
    }
}
