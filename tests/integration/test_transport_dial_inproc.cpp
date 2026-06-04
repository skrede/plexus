#include "plexus/io/peer_session.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <string_view>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_transport;
using plexus::io::handshake_fsm_config;
using session = plexus::io::peer_session<inproc_policy>;
using msg_forwarder = plexus::io::message_forwarder<inproc_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<inproc_policy>;

static_assert(plexus::io::transport_backend<inproc_transport<>, inproc_policy>);

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                .compatible_version_major = 1, .compatible_version_minor = 0};
}

// A DIALED two-node inproc link — no hand-dial connect_to. The transport's
// listen(ep) registers the accepting endpoint; dial(ep) mints a connected pair
// and fires the listener's on_accepted (the responder end, is_inbound_bootstrap
// =true) plus on_dialed (the dialer end, is_inbound_bootstrap=false). Both peer
// sessions are deferred in std::optional and constructed only once the channels
// land, so destruction unwinds channels-before-bus (the channels register on the
// bus at construction and must not outlive it). Member ORDER matters: bus,
// executor, transport, then the channels/sessions declared after.
struct dial_link
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};

    msg_forwarder req_messages;
    msg_forwarder resp_messages;
    rpc_forwarder req_procedures{ex, k_long_timeout};
    rpc_forwarder resp_procedures{ex, k_long_timeout};

    std::unique_ptr<inproc_channel<>> dialer_ch;
    std::unique_ptr<inproc_channel<>> accepted_ch;
    std::optional<session> requester;   // the dialer end
    std::optional<session> responder;   // the accepted end

    bool dial_refused{false};

    explicit dial_link(std::chrono::nanoseconds timeout = k_long_timeout)
    {
        transport.on_accepted([this, timeout](std::unique_ptr<inproc_channel<>> ch) {
            accepted_ch = std::move(ch);
            responder.emplace(*accepted_ch, ex, make_cfg(0x01), timeout,
                              resp_messages, resp_procedures, "requester-node", true);
            responder->start();
        });
        transport.on_dialed([this, timeout](std::unique_ptr<inproc_channel<>> ch) {
            dialer_ch = std::move(ch);
            requester.emplace(*dialer_ch, ex, make_cfg(0x02), timeout,
                              req_messages, req_procedures, "responder-node", false);
            requester->start();
        });
        transport.on_dial_failed([this](plexus::io::io_error) { dial_refused = true; });

        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive() { ex.drain(); }
};

}

TEST_CASE("inproc transport: a DIALED peer_session pair completes the handshake and mints epochs, looped",
          "[integration][transport][inproc]")
{
    constexpr int k_iterations = 100;
    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        dial_link l;
        l.drive();

        REQUIRE(l.requester.has_value());
        REQUIRE(l.responder.has_value());
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);
        REQUIRE(!l.dial_refused);

        // A further drive does not re-mint or re-install (install-once latch).
        const auto req_epoch = l.requester->session_id();
        l.drive();
        REQUIRE(l.requester->session_id() == req_epoch);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("inproc transport: dialing an unregistered endpoint surfaces connection_refused via on_dial_failed",
          "[integration][transport][inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};

    std::optional<plexus::io::io_error> failed;
    bool dialed = false;
    transport.on_dial_failed([&](plexus::io::io_error e) { failed = e; });
    transport.on_dialed([&](std::unique_ptr<inproc_channel<>>) { dialed = true; });

    transport.dial({"inproc", "nope"});
    ex.drain();

    REQUIRE(!dialed);
    REQUIRE(failed.has_value());
    REQUIRE(*failed == plexus::io::io_error::connection_refused);
}
