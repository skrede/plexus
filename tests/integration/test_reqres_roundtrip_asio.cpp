#include "test_reqres_roundtrip_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace reqres_asio_fixture;

TEST_CASE("req/res round-trips over real TCP loopback through plexus-asio, looped", "[integration][reqres][asio]")
{
    // Loop the FULL roundtrip >=100 times in-body, each iteration an independent
    // listener+client+io_context — a live-networking claim is never asserted from a
    // single run. A flaky frame surfaces as a mismatch on some iteration, not a
    // one-off pass. The ctest invocation is ALSO repeated >=3 process runs (the
    // CMake verify loop) for cross-process reproducibility.
    constexpr int k_iterations = 100;
    int           resolved     = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        live_rpc h;

        std::string seen_param;
        h.provider->serve("svc",
                          [&](std::span<const std::byte> param, forwarder::reply_fn &reply)
                          {
                              seen_param            = to_string(param);
                              const std::string ret = "return-" + seen_param;
                              reply(rpc_status::success, as_bytes(ret));
                          });

        rpc_status        got_status = rpc_status::error;
        std::string       got_return;
        const std::string param = "param-" + std::to_string(iter);
        h.caller.call(*h.caller_peer, "svc", as_bytes(param),
                      [&](rpc_status s, std::span<const std::byte> ret)
                      {
                          got_status = s;
                          got_return = to_string(ret);
                      });

        h.pump_until([&] { return got_status != rpc_status::error; });

        REQUIRE(seen_param == param); // provider saw the exact param
        REQUIRE(got_status == rpc_status::success);
        REQUIRE(got_return == "return-" + param); // caller matched the exact return
        ++resolved;
    }
    REQUIRE(resolved == k_iterations);
}
