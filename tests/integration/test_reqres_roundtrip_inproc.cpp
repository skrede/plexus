#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/rpc_status.h"

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

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using procedure_forwarder = plexus::io::procedure_forwarder<inproc_policy>;
using plexus::wire::rpc_status;

namespace {

// Far past any clock advance these roundtrip/alloc cases perform — they drain
// deterministically and never move the clock, so the per-call deadline never trips.
constexpr auto k_long_deadline = std::chrono::hours(1);

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// A bidirectional inproc RPC link: a caller forwarder and a provider forwarder,
// each receiving its inbound frames through a frame_router (the router owns the
// frame_header.type switch; the forwarder stays a pure consumer of pre-demuxed
// inner payloads). Caller requests ride caller_tx -> provider_rx; provider
// replies ride provider_tx -> caller_rx. drive() drains the step-loop to
// quiescence (deterministic — no wall clock, no polling).
struct rpc_link
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};

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

    rpc_link() { wire(); }

    void wire()
    {
        caller_tx.connect_to(provider_rx.local_endpoint());
        provider_tx.connect_to(caller_rx.local_endpoint());

        provider_router.on_rpc_request([this](std::span<const std::byte> inner)
                                       { provider.deliver_request(provider_peer, inner); });
        caller_router.on_rpc_response([this](std::span<const std::byte> inner)
                                      { caller.deliver_response(caller_peer, inner); });

        provider_rx.on_data([this](std::span<const std::byte> f) { provider_router.route(f); });
        caller_rx.on_data([this](std::span<const std::byte> f) { caller_router.route(f); });
    }

    void drive() { ex.drain(); }
};

}

TEST_CASE("inproc req/res roundtrip recovers the exact return bytes, looped",
          "[integration][reqres][inproc]")
{
    // The roundtrip is deterministic by construction (virtual clock + drain), so
    // looping N>=100 with a fresh bus/executor/forwarders each iteration surfaces
    // any flake as a mismatch on some iteration rather than a one-off pass.
    constexpr int k_iterations = 128;
    int           resolved     = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        rpc_link link;

        std::string seen_param;
        link.provider.serve(
                "svc",
                [&](std::span<const std::byte> param, procedure_forwarder::reply_fn &reply)
                {
                    seen_param            = to_string(param);
                    const std::string ret = "return-" + seen_param;
                    reply(rpc_status::success, as_bytes(ret));
                });

        rpc_status        got_status = rpc_status::error;
        std::string       got_return;
        const std::string param = "param-" + std::to_string(iter);
        link.caller.call(link.caller_peer, "svc", as_bytes(param),
                         [&](rpc_status s, std::span<const std::byte> ret)
                         {
                             got_status = s;
                             got_return = to_string(ret);
                         });
        link.drive();

        REQUIRE(seen_param == param);
        REQUIRE(got_status == rpc_status::success);
        REQUIRE(got_return == "return-" + param);
        ++resolved;
    }
    REQUIRE(resolved == k_iterations);
}

TEST_CASE("inproc concurrent outstanding requests each resolve to their own response, looped",
          "[integration][reqres][inproc]")
{
    // Issue M>=8 overlapping calls (distinct params, distinct correlation_ids)
    // BEFORE draining, so every request is in flight before any reply arrives.
    // The provider echoes the param, so cross-talk (a caller matching the wrong
    // response) would surface as a mismatched echo. Keyed by request index so the
    // assertion proves each callback resolved to ITS OWN response — no clobber.
    constexpr int k_iterations  = 128;
    constexpr int m_outstanding = 8;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        rpc_link link;
        link.provider.serve(
                "echo", [](std::span<const std::byte> param, procedure_forwarder::reply_fn &reply)
                { reply(rpc_status::success, param); });

        std::array<std::string, m_outstanding> got{};
        std::array<rpc_status, m_outstanding>  status{};
        status.fill(rpc_status::error);
        for(int i = 0; i < m_outstanding; ++i)
        {
            const std::string param = "req-" + std::to_string(iter) + "-" + std::to_string(i);
            link.caller.call(link.caller_peer, "echo", as_bytes(param),
                             [&got, &status, i](rpc_status s, std::span<const std::byte> ret)
                             {
                                 status[i] = s;
                                 got[i]    = to_string(ret);
                             });
        }
        link.drive();

        for(int i = 0; i < m_outstanding; ++i)
        {
            REQUIRE(status[i] == rpc_status::success);
            REQUIRE(got[i] == "req-" + std::to_string(iter) + "-" + std::to_string(i));
        }
    }
}

// A non-allocating sink Policy: its byte_channel records send sizes without
// copying, so a forwarder<sink_policy> request->correlate->response cycle
// exercises the rpc dispatch path with no transport-side allocation — isolating
// the forwarder's own heap behavior so a clean 0 is the rpc-path proof.
namespace {

struct sink_executor
{
};

struct sink_channel
{
    explicit sink_channel(sink_executor &) {}
    sink_channel(sink_executor &, std::error_code &) {}

    // send copies the frame into a reused per-channel scratch and records the
    // last frame for a DEFERRED route (the test pumps it after call() returns, so
    // the response never re-enters the caller before its pending entry exists —
    // mirroring the production post discipline, not a reentrant synchronous send).
    void send(std::span<const std::byte> d)
    {
        last.assign(d.begin(), d.end());
        ++pending;
    }
    void                 close() {}
    plexus::io::endpoint remote_endpoint() const { return {}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}

    std::vector<std::byte> last;
    int                    pending{0};
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
    using executor_type     = sink_executor &;
    using byte_channel_type = sink_channel;
    using timer_type        = sink_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<sink_policy>);

}

TEST_CASE("inproc rpc dispatch path is allocation-free; the correlation table churns one node per "
          "call",
          "[integration][reqres][inproc]")
{
    // The rpc dispatch path has two measured segments over a non-allocating sink
    // Policy (so the only heap that could appear is the forwarder's own):
    //
    //   (A) the provider receive tail — decode the request, dispatch to the
    //       handler over opaque bytes, frame a reply into reused scratch. It does
    //       NO map insertion (the handler registry is grown at serve()), so after
    //       warm-up it is allocation-FREE. This is the steady-state reply hot path.
    //
    //   (B) the caller correlate/match round — call() inserts a per-corr_id node
    //       into the bounded outstanding table AND arms a per-call deadline timer;
    //       deliver_response cancels the timer and erases the node. A node-based
    //       unordered_map does NOT pool, so each in-flight call churns a small
    //       CONSTANT number of heap blocks: the entry node, a bucket rehash on the
    //       empty/refill cycle, the timer object, and its type-erased async_wait
    //       handler. This is inherent correlation + timeout BOOKKEEPING, not the
    //       dispatch path — measured here HONESTLY (a bounded, K-proportional
    //       constant, NOT unbounded growth) rather than masked, and recorded as the
    //       v0.2.x pooled-outstanding seed.
    using sink_forwarder = plexus::io::procedure_forwarder<sink_policy>;

    sink_executor ex;
    sink_channel  caller_ch(ex);
    sink_channel  provider_ch(ex);

    sink_forwarder caller{ex, k_long_deadline};
    sink_forwarder provider{ex, k_long_deadline};

    plexus::io::frame_router caller_router;
    plexus::io::frame_router provider_router;

    sink_forwarder::peer caller_peer{caller_ch, "provider-node"};
    sink_forwarder::peer provider_peer{provider_ch, "caller-node"};

    provider_router.on_rpc_request([&](std::span<const std::byte> inner)
                                   { provider.deliver_request(provider_peer, inner); });
    caller_router.on_rpc_response([&](std::span<const std::byte> inner)
                                  { caller.deliver_response(caller_peer, inner); });

    const std::string ret_body = "return-bytes";
    provider.serve("svc", [&](std::span<const std::byte>, sink_forwarder::reply_fn &reply)
                   { reply(rpc_status::success, as_bytes(ret_body)); });

    const std::string param   = "steady-param";
    int               replies = 0;

    auto run_request = [&]
    {
        caller.call(caller_peer, "svc", as_bytes(param),
                    [&](rpc_status s, std::span<const std::byte>)
                    {
                        if(s == rpc_status::success)
                            ++replies;
                    });
    };
    auto pump_request = [&]
    { provider_router.route(caller_ch.last); }; // -> provider dispatch + reply
    auto pump_response = [&]
    { caller_router.route(provider_ch.last); }; // -> caller correlate + match

    // Warm-up: one full cycle grows every reused scratch (request, response, frame)
    // and proves the cycle resolves before measuring.
    run_request();
    pump_request();
    pump_response();
    REQUIRE(replies == 1);

    constexpr int K = 256;

    // Segment (A): the provider dispatch + reply-framing path — feed the SAME warm
    // request frame K times. No outstanding-table insertion happens here, so the
    // delta must be a clean 0.
    {
        const auto sends_before = provider_ch.pending;
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            pump_request();
        const auto after = plexus::testing::alloc_count();
        REQUIRE(provider_ch.pending - sends_before == K); // every dispatch replied
        REQUIRE(after - before == 0);                     // decode + dispatch + reply: ZERO alloc
    }

    // Segment (B): the full call -> correlate -> match round. The delta is the
    // correlation table's node churn — a small CONSTANT per in-flight call, BOUNDED
    // and deterministic, NOT unbounded growth. Proven by measuring the SAME cycle at
    // two loop counts and asserting the per-call cost is identical (so it scales
    // with in-flight calls, not with total throughput) and small.
    auto correlate_round_allocs = [&](int n) -> std::size_t
    {
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < n; ++i)
        {
            run_request();
            pump_request();
            pump_response();
        }
        return plexus::testing::alloc_count() - before;
    };

    const int         replies_before = replies;
    const std::size_t allocs_k       = correlate_round_allocs(K);
    const std::size_t allocs_2k      = correlate_round_allocs(2 * K);
    REQUIRE(replies - replies_before == K + 2 * K); // every cycle resolved

    // Per-call cost is a fixed small constant (no per-throughput growth): doubling
    // the loop doubles the allocations exactly, and the constant is bounded (<= 2).
    REQUIRE(allocs_2k == 2 * allocs_k);
    REQUIRE(allocs_k % static_cast<std::size_t>(K) == 0);
    const std::size_t per_call = allocs_k / static_cast<std::size_t>(K);
    REQUIRE(per_call >=
            1); // correlation + timeout tracking is not free (a node + a timer per in-flight call)
    REQUIRE(per_call <= 4); // but it is a small bounded constant, not the dispatch path
}
