#include "test_reqres_roundtrip_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace reqres_inproc_fixture;

TEST_CASE("inproc req/res roundtrip recovers the exact return bytes, looped", "[integration][reqres][inproc]")
{
    // The roundtrip is deterministic by construction (virtual clock + drain), so
    // looping N>=100 with a fresh bus/executor/forwarders each iteration surfaces
    // any flake as a mismatch on some iteration rather than a one-off pass.
    constexpr int k_iterations = 128;
    int resolved               = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        rpc_link link;

        std::string seen_param;
        link.provider.serve("svc",
                            [&](std::span<const std::byte> param, procedure_forwarder::reply_fn &reply)
                            {
                                seen_param            = to_string(param);
                                const std::string ret = "return-" + seen_param;
                                reply(rpc_status::success, as_bytes(ret));
                            });

        rpc_status got_status = rpc_status::error;
        std::string got_return;
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

TEST_CASE("inproc concurrent outstanding requests each resolve to their own response, looped", "[integration][reqres][inproc]")
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
        link.provider.serve("echo", [](std::span<const std::byte> param, procedure_forwarder::reply_fn &reply) { reply(rpc_status::success, param); });

        std::array<std::string, m_outstanding> got{};
        std::array<rpc_status, m_outstanding> status{};
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
    explicit sink_channel(sink_executor &)
    {
    }
    sink_channel(sink_executor &, std::error_code &)
    {
    }

    // send copies the frame into a reused per-channel scratch and records the
    // last frame for a DEFERRED route (the test pumps it after call() returns, so
    // the response never re-enters the caller before its pending entry exists —
    // mirroring the production post discipline, not a reentrant synchronous send).
    void send(std::span<const std::byte> d)
    {
        last.assign(d.begin(), d.end());
        ++pending;
    }
    void close()
    {
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return {};
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }
    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>)
    {
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>)
    {
    }

    std::vector<std::byte> last;
    int pending{0};
};

struct sink_timer
{
    explicit sink_timer(sink_executor &)
    {
    }
    sink_timer(sink_executor &, std::error_code &)
    {
    }
    void expires_after(std::chrono::milliseconds)
    {
    }
    void async_wait(plexus::detail::move_only_function<void(std::error_code)>)
    {
    }
    void cancel()
    {
    }
};

struct sink_policy
{
    using executor_type     = sink_executor &;
    using byte_channel_type = sink_channel;
    using timer_type        = sink_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn)
    {
        fn();
    }
};

static_assert(plexus::Policy<sink_policy>);

}
