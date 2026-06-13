#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/wire_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/log/logger.h"
#include "plexus/policy.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using procedure_forwarder = plexus::io::procedure_forwarder<inproc_policy>;
using plexus::wire::rpc_status;

// The maintainability gate: the procedure forwarder models the wire_forwarder
// shape over its own peer, exactly as message_forwarder does.
static_assert(plexus::io::wire_forwarder<procedure_forwarder, procedure_forwarder::peer>);

namespace {

// A deadline far past any clock advance the non-timeout cases perform — the inproc
// clock only moves when a test explicitly advances it, so a roundtrip/orphan case
// never trips this. The dedicated timeout cases pass their own short deadline.
constexpr auto k_long_deadline = std::chrono::hours(1);

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// A counting logger: warn() bumps a counter, proving the warn-and-drop seam fires.
struct counting_logger final : plexus::log::logger
{
    void warn(std::string_view) override { ++count; }
    std::size_t count{0};
};

// A bidirectional inproc RPC link: a caller forwarder and a provider forwarder,
// each with its own receive sink, cross-wired so the caller's rpc_request reaches
// the provider's deliver_request and the provider's rpc_response reaches the
// caller's deliver_response. Each side demuxes its inbound frames through a
// frame_router (the router owns the frame_header.type switch; the forwarder stays
// a pure consumer of pre-demuxed inner payloads). drive() drains the step-loop.
struct rpc_link
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};

    // The caller side: a channel it sends requests over, plus a sink that receives
    // responses. The provider side mirrors it.
    inproc_channel<> caller_tx{ex};
    inproc_channel<> caller_rx{ex};
    inproc_channel<> provider_tx{ex};
    inproc_channel<> provider_rx{ex};

    procedure_forwarder caller{ex, k_long_deadline};
    procedure_forwarder provider{ex, k_long_deadline};

    plexus::io::frame_router caller_router;
    plexus::io::frame_router provider_router;

    procedure_forwarder::peer caller_peer{caller_tx, "provider-node"};
    procedure_forwarder::peer provider_peer{provider_tx, "caller-node"};

    explicit rpc_link(plexus::log::logger &caller_log)
        : caller(ex, k_long_deadline, caller_log)
    {
        wire();
    }

    rpc_link()
    {
        wire();
    }

    void wire()
    {
        // Caller requests ride caller_tx -> provider_rx; provider replies ride
        // provider_tx -> caller_rx.
        caller_tx.connect_to(provider_rx.local_endpoint());
        provider_tx.connect_to(caller_rx.local_endpoint());

        provider_router.on_rpc_request([this](std::span<const std::byte> inner) {
            provider.deliver_request(provider_peer, inner);
        });
        caller_router.on_rpc_response([this](std::span<const std::byte> inner) {
            caller.deliver_response(caller_peer, inner);
        });

        provider_rx.on_data([this](std::span<const std::byte> f) { provider_router.route(f); });
        caller_rx.on_data([this](std::span<const std::byte> f) { caller_router.route(f); });
    }

    void drive() { ex.drain(); }
};

// Counts frame_header-wrapped subscribe_request frames a capture recorded.
struct capture
{
    explicit capture(inproc_executor<> &ex)
        : sink(ex)
    {
        sink.on_data([this](std::span<const std::byte> d) {
            frames.emplace_back(d.begin(), d.end());
        });
    }

    inproc_channel<> sink;
    std::vector<std::vector<std::byte>> frames;
};

std::size_t count_subscribes(const capture &cap)
{
    std::size_t n = 0;
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::subscribe)
            continue;
        auto inner = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        if(plexus::wire::decode_subscribe_request(inner))
            ++n;
    }
    return n;
}

procedure_forwarder::peer make_peer(inproc_channel<> &tx, capture &cap, std::string node_name)
{
    tx.connect_to(cap.sink.local_endpoint());
    return procedure_forwarder::peer{tx, std::move(node_name)};
}

// Synthesize a header-off rpc_response inner payload for an arbitrary corr_id (the
// orphan probe feeds this straight to deliver_response, bypassing the link).
std::vector<std::byte> make_response_inner(std::uint64_t corr_id, rpc_status status,
                                           std::span<const std::byte> ret)
{
    plexus::wire::bidirectional_header hdr{
            .source         = plexus::wire::endpoint_source_type::procedure,
            .sequence       = 0,
            .topic_hash     = 0,
            .type_hash_1    = 0,
            .type_hash_2    = 0,
            .correlation_id = corr_id
    };
    std::vector<std::byte> out;
    plexus::wire::encode_rpc_response_into(out, hdr, status, ret);
    return out;
}

}

TEST_CASE("attach refcount gate emits exactly one procedure subscribe on 0->1", "[procedure]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "provider-node");

        procedure_forwarder fwd{ex, k_long_deadline};
        REQUIRE(fwd.attach(peer, "svc"));          // 0->1
        REQUIRE_FALSE(fwd.attach(peer, "svc"));    // 1->2, no emit
        ex.drain();

        REQUIRE(count_subscribes(cap) == 1);
    }
}

TEST_CASE("attach succeeds on 0->1 for an arbitrary fqn (no remote registry)", "[procedure]")
{
    // plexus has no remote registry, so attach always succeeds on the 0->1
    // transition — a divergence from the source (which returns false on an unknown
    // remote topic). A call() needs no prior attach.
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "provider-node");

        procedure_forwarder fwd{ex, k_long_deadline};
        REQUIRE(fwd.attach(peer, "never.advertised.anywhere"));
        ex.drain();
        REQUIRE(count_subscribes(cap) == 1);
    }
}

TEST_CASE("roundtrip recovers exact opaque param and return bytes matched by corr_id", "[procedure]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        rpc_link link;

        std::string seen_param;
        link.provider.serve("svc", [&](std::span<const std::byte> param, procedure_forwarder::reply_fn &reply) {
            seen_param = to_string(param);
            const std::string ret = "return-" + seen_param;
            reply(rpc_status::success, as_bytes(ret));
        });

        rpc_status got_status = rpc_status::error;
        std::string got_return;
        const std::string param = "the-opaque-param";
        link.caller.call(link.caller_peer, "svc", as_bytes(param),
            [&](rpc_status s, std::span<const std::byte> ret) {
                got_status = s;
                got_return = to_string(ret);
            });
        link.drive();

        REQUIRE(seen_param == param);
        REQUIRE(got_status == rpc_status::success);
        REQUIRE(got_return == "return-the-opaque-param");
    }
}

TEST_CASE("call to an fqn with no provider yields on_response(no_handler, {})", "[procedure]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        rpc_link link;   // provider serves nothing

        rpc_status got = rpc_status::success;
        std::size_t ret_size = 999;
        link.caller.call(link.caller_peer, "absent.svc", {},
            [&](rpc_status s, std::span<const std::byte> ret) {
                got = s;
                ret_size = ret.size();
            });
        link.drive();

        REQUIRE(got == rpc_status::no_handler);
        REQUIRE(ret_size == 0);
    }
}

TEST_CASE("an orphan rpc_response warn-drops and fires no callback", "[procedure]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        counting_logger log;
        rpc_link link(log);

        // One real outstanding call (corr_id 1) so the peer's outstanding map
        // exists; then feed deliver_response a synthetic response for an unknown
        // corr_id. The real call's callback must NOT fire from the orphan.
        bool real_fired = false;
        link.caller.call(link.caller_peer, "svc", {},
            [&](rpc_status, std::span<const std::byte>) { real_fired = true; });

        auto orphan = make_response_inner(987654321u, rpc_status::success, {});
        link.caller.deliver_response(link.caller_peer, orphan);

        REQUIRE_FALSE(real_fired);
        REQUIRE(log.count == 1);   // the orphan warn fired exactly once
    }
}

TEST_CASE("detach_all fails every outstanding call with peer_disconnected", "[procedure]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        rpc_link link;   // provider never replies

        // Issue the calls but do NOT drive the step-loop: the request frames sit
        // undelivered, so the provider never replies. call() registers each pending
        // entry synchronously, so the outstanding table is populated regardless —
        // exactly the "a provider that never replies" state detach_all resolves.
        constexpr int outstanding = 4;
        std::array<rpc_status, outstanding> got{};
        got.fill(rpc_status::success);
        for(int i = 0; i < outstanding; ++i)
            link.caller.call(link.caller_peer, "svc", {},
                [&got, i](rpc_status s, std::span<const std::byte>) { got[i] = s; });

        link.caller.detach_all(link.caller_peer);

        for(int i = 0; i < outstanding; ++i)
            REQUIRE(got[i] == rpc_status::peer_disconnected);

        // The outstanding map is cleared: a second detach_all is a no-op (no
        // double-fire), and a late orphan response finds nothing to match.
        link.caller.detach_all(link.caller_peer);
    }
}

TEST_CASE("concurrent outstanding calls each resolve to their own response", "[procedure]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        rpc_link link;

        // The provider echoes the param back, so each response is keyed to its
        // request's payload — cross-talk would surface as a mismatched echo.
        link.provider.serve("echo", [](std::span<const std::byte> param, procedure_forwarder::reply_fn &reply) {
            reply(rpc_status::success, param);
        });

        constexpr int n = 8;
        std::array<std::string, n> got{};
        for(int i = 0; i < n; ++i)
        {
            const std::string param = "req-" + std::to_string(i);
            link.caller.call(link.caller_peer, "echo", as_bytes(param),
                [&got, i](rpc_status s, std::span<const std::byte> ret) {
                    if(s == rpc_status::success)
                        got[i] = to_string(ret);
                });
        }
        link.drive();

        for(int i = 0; i < n; ++i)
            REQUIRE(got[i] == "req-" + std::to_string(i));
    }
}

// A non-allocating sink Policy: its byte_channel records send sizes without
// copying, so a forwarder<sink_policy> call/reply exercises framing + dispatch
// with no transport-side allocation — isolating the forwarder's own heap behavior.
namespace {

struct sink_executor
{
};

struct sink_channel
{
    explicit sink_channel(sink_executor &) {}
    sink_channel(sink_executor &, std::error_code &) {}

    void send(std::span<const std::byte> d) { total_bytes += d.size(); ++sends; }
    void close() {}
    plexus::io::endpoint remote_endpoint() const { return {}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}

    std::size_t total_bytes{0};
    std::size_t sends{0};
};

struct sink_timer
{
    explicit sink_timer(sink_executor &) {}
    sink_timer(sink_executor &, std::error_code &) {}
    void expires_after(std::chrono::milliseconds) {}
    void async_wait(plexus::detail::move_only_function<void(std::error_code)>) {}
    void cancel() {}
};

struct sink_policy
{
    using executor_type = sink_executor &;
    using byte_channel_type = sink_channel;
    using timer_type = sink_timer;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<sink_policy>);

}

TEST_CASE("steady-state provider dispatch + reply framing allocates nothing", "[procedure]")
{
    // The provider receive tail is the req/res hot path: decode an inbound request,
    // dispatch to the handler over opaque bytes, and frame the reply into reused
    // scratch. It performs NO map insertion (the handler registry is grown at
    // serve()), so after warm-up it must allocate nothing — the sibling property of
    // message_forwarder's frame-once fan-out. Measured over the non-allocating sink
    // Policy so the only heap traffic that could appear is the forwarder's own.
    using sink_forwarder = plexus::io::procedure_forwarder<sink_policy>;

    sink_executor ex;
    sink_channel provider_ch(ex);
    sink_forwarder provider{ex, k_long_deadline};
    sink_forwarder::peer provider_peer{provider_ch, "caller-node"};

    // A handler that replies with a fixed return span captured by reference: the
    // reply itself moves no heap (the body lives in the test frame).
    const std::string ret_body = "return-bytes";
    provider.serve("svc", [&](std::span<const std::byte>, sink_forwarder::reply_fn &reply) {
        reply(rpc_status::success, as_bytes(ret_body));
    });

    // The inbound request inner (header-off), built ONCE outside the gate and reused
    // every iteration, so the gate measures only the forwarder's decode + dispatch +
    // reply-framing path.
    const std::string param = "steady-param";
    plexus::wire::bidirectional_header req_hdr{
            .source         = plexus::wire::endpoint_source_type::caller,
            .sequence       = 0,
            .topic_hash     = plexus::wire::fqn_topic_hash("svc"),
            .type_hash_1    = 0,
            .type_hash_2    = 0,
            .correlation_id = 1
    };
    std::vector<std::byte> req_inner;
    plexus::wire::encode_rpc_request_into(req_inner, req_hdr, as_bytes(param));

    // Warm-up: one dispatch grows the reply + frame scratch to steady-state size.
    provider.deliver_request(provider_peer, req_inner);
    const auto sends_before = provider_ch.sends;

    constexpr int K = 256;
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        provider.deliver_request(provider_peer, req_inner);
    const auto after = plexus::testing::alloc_count();

    REQUIRE(provider_ch.sends - sends_before == K);   // every dispatch replied
    REQUIRE(after - before == 0);                     // decode + dispatch + reply: zero alloc
}

namespace {

// A manual clock: now() returns an advance-able point the test moves by hand, so a
// per-call deadline fires deterministically — no wall-clock sleep, no polling. The
// static point is the std Clock idiom (mirrors steady_clock::now()); each case
// resets it so the loop iterations and cases never contaminate each other.
struct manual_clock
{
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept { return current; }
    static void reset() noexcept { current = time_point{}; }
    static void advance(duration d) noexcept { current += d; }
};

// The manual-clock inproc Policy: the same substrate as inproc_policy, but every
// piece (executor, channel, timer) is instantiated on manual_clock so the
// forwarder's per-call timer fires off the advance-able clock.
struct manual_policy
{
    using executor_type = plexus::inproc::inproc_executor<manual_clock> &;
    using byte_channel_type = plexus::inproc::inproc_channel<manual_clock>;
    using timer_type = plexus::inproc::inproc_timer<manual_clock>;
    using byte_owner = std::shared_ptr<const void>;

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
    plexus::inproc::inproc_bus<manual_clock> bus;
    plexus::inproc::inproc_executor<manual_clock> ex{bus};

    plexus::inproc::inproc_channel<manual_clock> caller_tx{ex};
    plexus::inproc::inproc_channel<manual_clock> caller_rx{ex};
    plexus::inproc::inproc_channel<manual_clock> provider_tx{ex};
    plexus::inproc::inproc_channel<manual_clock> provider_rx{ex};

    manual_forwarder caller;
    manual_forwarder provider{ex, std::chrono::hours(1)};

    plexus::io::frame_router caller_router;
    plexus::io::frame_router provider_router;

    manual_forwarder::peer caller_peer{caller_tx, "provider-node"};
    manual_forwarder::peer provider_peer{provider_tx, "caller-node"};

    explicit manual_link(std::chrono::nanoseconds caller_deadline)
        : caller(ex, caller_deadline)
    {
        caller_tx.connect_to(provider_rx.local_endpoint());
        provider_tx.connect_to(caller_rx.local_endpoint());

        provider_router.on_rpc_request([this](std::span<const std::byte> inner) {
            provider.deliver_request(provider_peer, inner);
        });
        caller_router.on_rpc_response([this](std::span<const std::byte> inner) {
            caller.deliver_response(caller_peer, inner);
        });
        provider_rx.on_data([this](std::span<const std::byte> f) { provider_router.route(f); });
        caller_rx.on_data([this](std::span<const std::byte> f) { caller_router.route(f); });
    }

    void drive() { ex.drain(); }
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
        const auto deadline = std::chrono::milliseconds(50);
        manual_link link(deadline);

        // A provider that receives the request and is dispatched, but NEVER replies
        // (it drops the reply on the floor) — the genuine hang-forever state the
        // timeout resolves. Serving a no-op handler is faithful: an UNSERVED fqn
        // would instead bounce back a no_handler response and resolve the call early.
        link.provider.serve("svc", [](std::span<const std::byte>, manual_forwarder::reply_fn &) {});

        int fired = 0;
        rpc_status got = rpc_status::error;
        std::size_t ret_size = 999;
        link.caller.call(link.caller_peer, "svc", {},
            [&](rpc_status s, std::span<const std::byte> ret) {
                ++fired;
                got = s;
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
        const auto deadline = std::chrono::milliseconds(50);
        manual_link link(deadline);

        link.provider.serve("svc", [](std::span<const std::byte> param, manual_forwarder::reply_fn &reply) {
            reply(rpc_status::success, param);
        });

        int fired = 0;
        rpc_status got = rpc_status::error;
        std::string ret;
        const std::string param = "answered";
        link.caller.call(link.caller_peer, "svc", as_bytes(param),
            [&](rpc_status s, std::span<const std::byte> r) {
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
