#include "test_udp_reproducibility_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace udp_repro_fixture;

TEST_CASE("integration udp reproducibility: best_effort delivers the clean path every iteration", "[integration][udp][reproducibility]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::optional<std::string> got;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b) { got = str_of(b); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);

        const std::string payload = "be-" + std::to_string(iter);
        dialed->send(bytes_of(payload));
        pump_until(io, [&] { return got.has_value(); });
        REQUIRE(got.has_value());
        REQUIRE(*got == payload);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
