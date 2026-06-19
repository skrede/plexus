#include "test_shared_executor_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace shared_executor_fixture;

TEST_CASE("mdnspp re-advertise on a live server updates the record in place", "[integration][asio]")
{
    ::asio::io_context io;

    pmdns::mdnspp_discovery         advertiser(io, "_plexus._tcp.local.");
    plexus::discovery::service_info local{"inplace._plexus._tcp.local.",
                                          {"tcp", "127.0.0.1:5566"},
                                          {{"node_id", "0022"}, {"plexus/tcp/port", "5566"}}};
    advertiser.advertise(local);

    // Let the first announce settle so the server is live (started + probed) before the
    // in-place update — update_service_info requires a running server.
    auto settle = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while(std::chrono::steady_clock::now() < settle)
        io.poll();

    // Re-advertise the SAME name with an ADDED key: the live server updates in place.
    local.metadata.emplace_back("plexus/schema", "1");
    advertiser.advertise(local);

    pmdns::mdnspp_discovery                          discovery(io, "_plexus._tcp.local.");
    int                                              resolved_count = 0;
    std::vector<std::pair<std::string, std::string>> resolved_metadata;
    discovery.browse(
            [&](const plexus::discovery::service_info &svc)
            {
                ++resolved_count;
                if(!svc.metadata.empty())
                    resolved_metadata = svc.metadata;
            });

    // Poll past the browse silence timeout (mdnspp default 3s) so aggregation completes.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while(std::chrono::steady_clock::now() < deadline &&
          (resolved_count == 0 || read_card_value(resolved_metadata, "plexus/schema").empty()))
        io.poll();

    discovery.stop();
    advertiser.stop();
    io.poll();

    // The browser saw the UPDATED record: the original keys AND the added one, all
    // through the in-place update path (no server re-construction).
    REQUIRE(read_card_value(resolved_metadata, "node_id") == "0022");
    REQUIRE(read_card_value(resolved_metadata, "plexus/tcp/port") == "5566");
    REQUIRE(read_card_value(resolved_metadata, "plexus/schema") == "1");
}
