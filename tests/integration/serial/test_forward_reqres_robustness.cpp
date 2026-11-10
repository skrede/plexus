// The consumer-side origination seam's robustness, over the same cold serial+TCP relay fixture. Two
// arms: a target reachable ONLY through a relay whose whole session is destroyed fails a caller<>
// cleanly within a bounded pumped window (no hang, no manufactured route), and a single caller<> call
// executes the origin's served handler EXACTLY once (the direct attempt answers unserved without
// running it, and the single forwarded re-issue runs it once — no direct+forwarded double-fire).

#include "test_forward_serial_e2e_common.h"

#include "plexus/caller.h"
#include "plexus/procedure.h"
#include "plexus/call_error.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <cstddef>
#include <optional>
#include <string_view>
#include <system_error>

using namespace forward_serial_e2e_fixture;

namespace {

constexpr std::string_view k_procedure = "sensor/echo";

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

}

TEST_CASE("forward_reqres_robustness: a caller through a destroyed relay session fails cleanly within a bounded window",
          "[integration][serial][relay][reqres][forward_reqres_robustness]")
{
    cold_cluster cluster;
    cluster.bring_up_serial();
    REQUIRE(connected(*cluster.relay, cluster.origin_id));

    plexus::procedure<> echo{*cluster.origin, k_procedure,
                             [&](std::span<const std::byte> param, plexus::io::reply_fn &reply)
                             { reply(plexus::wire::rpc_status::success, param); }};

    cluster.bring_up_tcp();
    REQUIRE(connected(cluster.consumer, cluster.relay_id));
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });
    REQUIRE(find_participant(cluster.consumer, cluster.origin_id) != nullptr);

    // The whole relay node — both its sessions — is destroyed. The origin is now reachable ONLY through
    // a relay that is gone, so the request can never complete over any live route.
    cluster.kill_relay();

    std::optional<plexus::expected<plexus::reply, std::error_code>> result;
    plexus::call_options opts;
    opts.deadline = std::chrono::milliseconds(300);
    plexus::caller<> call{cluster.consumer, k_procedure};
    call.call(as_bytes(std::string{"ping"}), opts,
              [&](plexus::expected<plexus::reply, std::error_code> r) { result = std::move(r); });

    // A BOUNDED clean fail: the completion fires (pump_until caps at 5s) and it is a failure, never a
    // success and never a hang — the deadline resolves the dead route.
    cluster.pump([&] { return result.has_value(); });
    REQUIRE(result.has_value());
    REQUIRE_FALSE(result->has_value());
    REQUIRE_FALSE(connected(cluster.consumer, cluster.origin_id));
}

TEST_CASE("forward_reqres_robustness: a single caller call runs the origin's served handler exactly once",
          "[integration][serial][relay][reqres][forward_reqres_robustness]")
{
    cold_cluster cluster;
    cluster.bring_up_serial();
    REQUIRE(connected(*cluster.relay, cluster.origin_id));

    int invocations = 0;
    plexus::procedure<> echo{*cluster.origin, k_procedure,
                             [&](std::span<const std::byte> param, plexus::io::reply_fn &reply)
                             {
                                 ++invocations;
                                 reply(plexus::wire::rpc_status::success, param);
                             }};

    cluster.bring_up_tcp();
    REQUIRE(connected(cluster.consumer, cluster.relay_id));
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });

    std::optional<bool> ok;
    std::string response;
    plexus::caller<> call{cluster.consumer, k_procedure};
    call.call(as_bytes(std::string{"ping"}),
              [&](plexus::expected<plexus::reply, std::error_code> r)
              {
                  ok = r.has_value();
                  if(r)
                      response = to_string(r->bytes);
              });
    cluster.pump([&] { return ok.has_value(); });

    // The direct attempt reaches the relay and answers unserved WITHOUT executing the origin's handler;
    // the single forwarded re-issue executes it once. One caller call, one execution.
    REQUIRE(ok.has_value());
    REQUIRE(*ok);
    REQUIRE(response == "ping");
    REQUIRE(invocations == 1);
    REQUIRE_FALSE(connected(cluster.consumer, cluster.origin_id));
}
