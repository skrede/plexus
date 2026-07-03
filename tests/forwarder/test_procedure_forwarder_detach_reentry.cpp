#include "test_procedure_forwarder_common.h"

#include <array>
#include <string>
#include <optional>

using namespace procedure_forwarder_fixture;

TEST_CASE("detach_all fires every outstanding reply exactly once even when a callback re-issues a call that rehashes the table", "[procedure][detach]")
{
    for(int iter = 0; iter < 50; ++iter)
    {
        rpc_link link; // the provider never replies; the request frames sit undelivered

        // Enough outstanding calls to one peer that the distinct-peer retries issued from the reply
        // callbacks grow the outer table past its rehash threshold — the pre-fix path holds an
        // iterator into that table across the callbacks and erases through it afterward. call()
        // registers each pending entry synchronously, so the table is populated without driving.
        constexpr int outstanding = 32;

        std::array<int, outstanding> orig_fired{};
        std::array<rpc_status, outstanding> orig_status{};
        orig_status.fill(rpc_status::success);

        int same_peer_retries  = 0; // replies re-issued to the dying peer
        int other_peer_retries = 0; // replies re-issued to a fresh peer

        for(int i = 0; i < outstanding; ++i)
        {
            link.caller.call(link.caller_peer, "svc", {},
                             [&, i](rpc_status s, std::span<const std::byte>)
                             {
                                 ++orig_fired[i];
                                 orig_status[i] = s;
                                 // Re-issue one call to the SAME (dying) peer and one to a DISTINCT peer
                                 // from inside the reply — the re-entrancy the fix must tolerate.
                                 procedure_forwarder::peer other{link.caller_tx, "other-" + std::to_string(i)};
                                 link.caller.call(link.caller_peer, "svc", {}, [&](rpc_status, std::span<const std::byte>) { ++same_peer_retries; });
                                 link.caller.call(other, "svc", {}, [&](rpc_status, std::span<const std::byte>) { ++other_peer_retries; });
                             });
        }

        link.caller.detach_all(link.caller_peer);

        // Every original reply fired exactly once with peer_disconnected — no double-fire, no missed
        // entry, no crash under asan/ubsan (the pre-fix code rehashes the map mid-iteration).
        for(int i = 0; i < outstanding; ++i)
        {
            REQUIRE(orig_fired[i] == 1);
            REQUIRE(orig_status[i] == rpc_status::peer_disconnected);
        }

        // The retries were tracked in the rebuilt table: draining them with a second peer-death round
        // fires each exactly once. The same-peer retries all landed under the dying peer's rebuilt
        // entry; the distinct-peer retries under their own keys.
        link.caller.detach_all(link.caller_peer);
        for(int i = 0; i < outstanding; ++i)
        {
            procedure_forwarder::peer other{link.caller_tx, "other-" + std::to_string(i)};
            link.caller.detach_all(other);
        }
        REQUIRE(same_peer_retries == outstanding);
        REQUIRE(other_peer_retries == outstanding);
    }
}
