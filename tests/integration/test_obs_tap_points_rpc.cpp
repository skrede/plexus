#include "test_obs_tap_points_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace obs_tap_fixture;

TEST_CASE("obs tap: an rpc round-trip fires call once (caller), serve once (provider), reply once "
          "(caller)",
          "[integration][observer][tap]")
{
    using plexus::wire::rpc_status;

    manual_clock::reset();
    rpc_net            net;
    recording_observer caller_rec;
    recording_observer provider_rec;
    net.caller.add_observer(caller_rec);
    net.provider.add_observer(provider_rec);
    net.drive(); // settle the dial + handshake
    REQUIRE(net.caller.is_connected(rpc_net::provider_id()));

    net.provider.procedures().serve(
            rpc_net::k_proc,
            [](std::span<const std::byte>,
               plexus::io::procedure_forwarder<manual_policy>::reply_fn &reply)
            { reply(plexus::wire::rpc_status::success, {}); });

    rpc_status got = rpc_status::error;
    net.caller.call(rpc_net::provider_id(), rpc_net::k_proc, as_bytes(std::string{"ping"}),
                    [&](rpc_status s, std::span<const std::byte>) { got = s; });

    // Posted-not-inline: the call tap fans out on the executor, so the caller's counter
    // is still zero synchronously at the call() return.
    REQUIRE(caller_rec.for_topic(rpc_net::k_proc).rpc_call == 0);

    net.drive(); // pump the request, the dispatch + reply, and the posted tap turns

    REQUIRE(caller_rec.for_topic(rpc_net::k_proc).rpc_call == 1); // call() once on the caller
    REQUIRE(provider_rec.for_topic(rpc_net::k_proc).rpc_serve ==
            1); // deliver_request() once on the provider
    REQUIRE(caller_rec.for_topic(rpc_net::k_proc).rpc_reply ==
            1); // deliver_response() once on the caller
    REQUIRE(got == rpc_status::success);
}
