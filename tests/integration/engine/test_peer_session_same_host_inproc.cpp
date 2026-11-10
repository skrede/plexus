// The same-host eligibility verdict: after a handshake completes, each peer
// has compared the OTHER end's advertised fingerprint to its own and recorded the
// verdict on its peer_context. This oracle stands up a two-node inproc link with
// controllable local fingerprints and proves the three cases:
//   * equal non-null fingerprints  -> both record same_host = true
//   * distinct fingerprints        -> both record same_host = false (cross-host)
//   * one end advertises null (0)   -> the OTHER end records false (the null-guard)
// and that a not-same-host pair never resolves to the shared-memory medium (the
// eligibility gate), regardless of a qualifying dispatch hint. The fingerprint rides
// the handshake frame as the appended field; no separate exchange. Looped for the
// inproc determinism the suite expects.

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/io/host_fingerprint.h"
#include "plexus/shm/shm_selection.h"
#include "plexus/io/dispatch_hint.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <memory>
#include <optional>
#include <cstdint>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::inproc::inproc_policy;
using plexus::io::handshake_fsm_config;
using plexus::io::host_fingerprint;
using session       = plexus::io::peer_session<inproc_policy>;
using msg_forwarder = plexus::io::message_forwarder<inproc_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<inproc_policy>;

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);

handshake_fsm_config make_cfg(std::uint8_t id_seed, std::uint64_t fingerprint)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{
            .self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0, .local_fingerprint = host_fingerprint{fingerprint}};
}

// A two-node inproc link whose two ends advertise the given fingerprints. The
// requester (dialer) advertises req_fp; the responder (accepted bootstrap) advertises
// resp_fp. After drive() both have completed and recorded their same-host verdict.
struct session_link
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};

    plexus::log::null_logger sink;
    msg_forwarder req_messages{sink}, resp_messages{sink};
    rpc_forwarder req_procedures{ex, k_long_timeout, sink};
    rpc_forwarder resp_procedures{ex, k_long_timeout, sink};

    plexus::io::peer_context<inproc_policy> req_ctx, resp_ctx;
    std::optional<session> requester, responder;

    session_link(std::uint64_t req_fp, std::uint64_t resp_fp)
    {
        transport.on_accepted(
                [this, resp_fp](std::unique_ptr<inproc_channel<>> ch)
                {
                    resp_ctx.channel   = std::move(ch);
                    resp_ctx.node_name = "requester-node";
                    responder.emplace(resp_ctx, ex, make_cfg(0x01, resp_fp), k_long_timeout, resp_messages, resp_procedures, true, sink);
                    responder->start();
                });
        transport.on_dialed(
                [this, req_fp](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &)
                {
                    req_ctx.channel   = std::move(ch);
                    req_ctx.node_name = "responder-node";
                    requester.emplace(req_ctx, ex, make_cfg(0x02, req_fp), k_long_timeout, req_messages, req_procedures, false, sink);
                    requester->start();
                });
        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive()
    {
        ex.drain();
    }
};

constexpr std::uint64_t k_host_a = 0xA11CE0FFEEull;
constexpr std::uint64_t k_host_b = 0xB0B0B0B0B0ull;

}

TEST_CASE("inproc peer_session: equal non-null fingerprints record same-host on both ends, looped", "[integration][peer_session][same_host][inproc]")
{
    for(int iter = 0; iter < 50; ++iter)
    {
        session_link l{k_host_a, k_host_a};
        l.drive();
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        CHECK(l.req_ctx.same_host);  // the dialer saw the responder's equal fingerprint
        CHECK(l.resp_ctx.same_host); // the responder saw the dialer's equal fingerprint
    }
}

TEST_CASE("inproc peer_session: distinct fingerprints record NOT same-host (cross-host), looped", "[integration][peer_session][same_host][inproc]")
{
    for(int iter = 0; iter < 50; ++iter)
    {
        session_link l{k_host_a, k_host_b};
        l.drive();
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        CHECK_FALSE(l.req_ctx.same_host);
        CHECK_FALSE(l.resp_ctx.same_host);
    }
}

TEST_CASE("inproc peer_session: a peer advertising a null fingerprint is NOT same-host (fail "
          "closed), looped",
          "[integration][peer_session][same_host][inproc]")
{
    for(int iter = 0; iter < 50; ++iter)
    {
        // The responder advertises a null (zero) fingerprint; the requester carries a
        // real one. The requester must NOT claim co-location off a null advertisement,
        // and a node that itself has no fingerprint (the responder) is never same-host.
        session_link l{k_host_a, 0};
        l.drive();
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        CHECK_FALSE(l.req_ctx.same_host);  // peer advertised null -> fail closed
        CHECK_FALSE(l.resp_ctx.same_host); // our own fingerprint is null -> never same-host
    }
}

TEST_CASE("the same-host verdict gates shared-memory eligibility: a non-same-host pair never "
          "attempts a ring acquire",
          "[integration][peer_session][same_host][inproc]")
{
    using plexus::shm::select_same_host_medium;
    using plexus::shm::same_host_medium;
    using plexus::io::dispatch_hint;

    session_link l{k_host_a, k_host_b}; // distinct hosts
    l.drive();
    REQUIRE_FALSE(l.req_ctx.same_host);

    // Even with a qualifying hint, a non-same-host pair resolves to the stream — the
    // eligibility gate denies the ring acquire. The verdict the session recorded is the
    // co-location fact the selection consumes (here mirrored through the pure decision).
    const host_fingerprint local{k_host_a}, peer{k_host_b};
    CHECK(select_same_host_medium(peer, local, dispatch_hint::frequent) == same_host_medium::stream);

    // The same-host pair WITH a hint is the only combination that resolves to shm.
    CHECK(select_same_host_medium(host_fingerprint{k_host_a}, host_fingerprint{k_host_a}, dispatch_hint::frequent) == same_host_medium::shm);
    // Same-host but NO hint stays on the stream (the hint gates the attempt).
    CHECK(select_same_host_medium(host_fingerprint{k_host_a}, host_fingerprint{k_host_a}, dispatch_hint::none) == same_host_medium::stream);
}
