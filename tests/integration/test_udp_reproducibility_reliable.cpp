#include "test_udp_reproducibility_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace udp_repro_fixture;

TEST_CASE(
        "integration udp reproducibility: reliable-ARQ delivers in order over loss every iteration",
        "[integration][udp][reproducibility]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context   io;
        pasio::udp_transport server{io, pasio::udp_channel::default_max_payload,
                                    pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs,
                                    fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::string>            delivered;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b)
                                      { delivered.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        relay link{io, server.port()};
        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(link.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);

        // Drop the 2nd and 4th segments once each: the ARQ must retransmit and the
        // receiver HOL must still deliver all 5 in publish order.
        link.data_script = {action::pass, action::drop, action::pass, action::drop, action::pass};

        std::vector<std::string> sent;
        for(int i = 0; i < 5; ++i)
        {
            const std::string p = "rel-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            dialed->send(bytes_of(p)); // reliable mode: send() drives the ARQ
        }
        pump_until(io, [&] { return delivered.size() == 5; });
        REQUIRE(delivered == sent); // in order, complete, over loss — every iteration
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
