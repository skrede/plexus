#include "test_procedure_forwarder_common.h"

using namespace procedure_forwarder_fixture;

TEST_CASE("attach refcount gate emits exactly one procedure subscribe on 0->1", "[procedure]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "provider-node");

        plexus::log::null_logger sink;
        procedure_forwarder fwd{ex, k_long_deadline, sink};
        REQUIRE(fwd.attach(peer, "svc"));       // 0->1
        REQUIRE_FALSE(fwd.attach(peer, "svc")); // 1->2, no emit
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

        plexus::log::null_logger sink;
        procedure_forwarder fwd{ex, k_long_deadline, sink};
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
        link.provider.serve("svc",
                            [&](std::span<const std::byte> param, procedure_forwarder::reply_fn &reply)
                            {
                                seen_param            = to_string(param);
                                const std::string ret = "return-" + seen_param;
                                reply(rpc_status::success, as_bytes(ret));
                            });

        rpc_status got_status = rpc_status::error;
        std::string got_return;
        const std::string param = "the-opaque-param";
        link.caller.call(link.caller_peer, "svc", as_bytes(param),
                         [&](rpc_status s, std::span<const std::byte> ret)
                         {
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
        rpc_link link; // provider serves nothing

        rpc_status got       = rpc_status::success;
        std::size_t ret_size = 999;
        link.caller.call(link.caller_peer, "absent.svc", {},
                         [&](rpc_status s, std::span<const std::byte> ret)
                         {
                             got      = s;
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
        link.caller.call(link.caller_peer, "svc", {}, [&](rpc_status, std::span<const std::byte>) { real_fired = true; });

        auto orphan = make_response_inner(987654321u, rpc_status::success, {});
        link.caller.deliver_response(link.caller_peer, orphan);

        REQUIRE_FALSE(real_fired);
        REQUIRE(log.count == 1); // the orphan warn fired exactly once
    }
}

TEST_CASE("detach_all fails every outstanding call with peer_disconnected", "[procedure]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        rpc_link link; // provider never replies

        // Issue the calls but do NOT drive the step-loop: the request frames sit
        // undelivered, so the provider never replies. call() registers each pending
        // entry synchronously, so the outstanding table is populated regardless —
        // exactly the "a provider that never replies" state detach_all resolves.
        constexpr int outstanding = 4;
        std::array<rpc_status, outstanding> got{};
        got.fill(rpc_status::success);
        for(int i = 0; i < outstanding; ++i)
            link.caller.call(link.caller_peer, "svc", {}, [&got, i](rpc_status s, std::span<const std::byte>) { got[i] = s; });

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
        link.provider.serve("echo", [](std::span<const std::byte> param, procedure_forwarder::reply_fn &reply) { reply(rpc_status::success, param); });

        constexpr int n = 8;
        std::array<std::string, n> got{};
        for(int i = 0; i < n; ++i)
        {
            const std::string param = "req-" + std::to_string(i);
            link.caller.call(link.caller_peer, "echo", as_bytes(param),
                             [&got, i](rpc_status s, std::span<const std::byte> ret)
                             {
                                 if(s == rpc_status::success)
                                     got[i] = to_string(ret);
                             });
        }
        link.drive();

        for(int i = 0; i < n; ++i)
            REQUIRE(got[i] == "req-" + std::to_string(i));
    }
}
