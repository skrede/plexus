#include "test_reqres_roundtrip_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace reqres_asio_fixture;

TEST_CASE("concurrent outstanding req/res over real TCP each resolve to their own response, looped", "[integration][reqres][asio]")
{
    // The key staged-context stress over REAL asio TCP: issue M>=8 overlapping
    // calls (distinct correlation_ids) BEFORE pumping, so multiple requests are
    // in flight before any response arrives. The provider echoes the param, so a
    // clobber (the caller matching the wrong response, or the provider's staged
    // reply context being overwritten across dispatches) would surface as a
    // mismatched echo on some call. Asserting each callback resolved to ITS OWN
    // response proves no cross-talk over the wire.
    constexpr int k_iterations  = 100;
    constexpr int m_outstanding = 8;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        live_rpc h;
        h.provider->serve("echo", [](std::span<const std::byte> param, forwarder::reply_fn &reply) { reply(rpc_status::success, param); });

        std::array<std::string, m_outstanding> got{};
        std::array<rpc_status, m_outstanding> status{};
        status.fill(rpc_status::error);
        for(int i = 0; i < m_outstanding; ++i)
        {
            const std::string param = "req-" + std::to_string(iter) + "-" + std::to_string(i);
            h.caller.call(*h.caller_peer, "echo", as_bytes(param),
                          [&got, &status, i](rpc_status s, std::span<const std::byte> ret)
                          {
                              status[i] = s;
                              got[i]    = to_string(ret);
                          });
        }

        int done = 0;
        h.pump_until(
                [&]
                {
                    done = 0;
                    for(int i = 0; i < m_outstanding; ++i)
                        if(status[i] != rpc_status::error)
                            ++done;
                    return done == m_outstanding;
                });

        for(int i = 0; i < m_outstanding; ++i)
        {
            REQUIRE(status[i] == rpc_status::success);
            REQUIRE(got[i] == "req-" + std::to_string(iter) + "-" + std::to_string(i));
        }
    }
}
