// over-limit: one cohesive downgrade/posture-mismatch refusal matrix; the both-ways refusal cells
// share the one transcript-bound two-peer_session fail-closed harness, so splitting them scatters
// that shared fixture The downgrade + posture-mismatch refusal oracle: two peer_sessions over the
// inproc backend prove a forced cipher/version downgrade is refused with the distinguishing
// downgrade_refused event in BOTH directions, and a secured-vs-plain posture mismatch is refused
// with posture_mismatch in BOTH directions — fail-closed, no silent plaintext fallback. No OpenSSL:
// a transcript-aware test attach_policy stands in for the transcript-bound proof recompute (a
// tampered offer changes the digest, so the policy refuses), keeping the bridge logic in the
// plexus::plexus-only core.

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/security_seam.h"
#include "plexus/io/security_event.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/security/attach_policy.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::inproc::inproc_policy;
using plexus::io::handshake_fsm_config;
using plexus::io::security_event;
using plexus::io::security_kind;
using plexus::io::security_seam;
using plexus::io::security_negotiation;
using plexus::io::lifecycle_event;
using plexus::io::lifecycle_edge;
using plexus::io::handshake_outcome;
using plexus::io::security::attach_policy;
using plexus::io::security::attach_facts;
using session       = plexus::io::peer_session<inproc_policy>;
using msg_forwarder = plexus::io::message_forwarder<inproc_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<inproc_policy>;

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);

handshake_fsm_config make_cfg(std::uint8_t id_seed, const attach_policy *policy)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{
            .self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0, .local_fingerprint = {}, .attach_policy = policy};
}

// The transcript-bound proof stand-in: it refuses any facts whose transcript_digest is
// non-zero (a tampered offer produces a non-zero fold under the engaged seam), modeling
// the real proof failure a stripped cipher offer causes. An all-zero digest (the honest,
// untampered transcript under the test fold) admits.
struct transcript_policy final : public attach_policy
{
    [[nodiscard]] bool decide(const attach_facts &f) const noexcept override
    {
        for(auto b : f.transcript_digest)
            if(b != std::byte{0})
                return false;
        return true;
    }
};

// An always-admit policy (the honest path under no tamper).
struct admit_policy final : public attach_policy
{
    [[nodiscard]] bool decide(const attach_facts &) const noexcept override
    {
        return true;
    }
};

// A seam whose transcript fold returns a NON-ZERO digest — the "tampered transcript"
// fixture: the gate (transcript_policy) refuses it as a downgrade.
security_seam tampered_seam()
{
    security_seam s;
    s.transcript = [](std::span<const std::byte>, std::span<std::byte, 32> out)
    {
        for(auto &b : out)
            b = std::byte{0xab};
        return true;
    };
    return s;
}

// A seam whose transcript fold returns an ALL-ZERO digest — the honest, untampered path.
security_seam honest_seam()
{
    security_seam s;
    s.transcript = [](std::span<const std::byte>, std::span<std::byte, 32> out)
    {
        for(auto &b : out)
            b = std::byte{0};
        return true;
    };
    return s;
}

int count_kind(const std::vector<security_event> &evs, security_kind k)
{
    int n = 0;
    for(const auto &e : evs)
        if(e.kind == k)
            ++n;
    return n;
}

int count_rejected(const std::vector<lifecycle_event> &evs)
{
    int n = 0;
    for(const auto &e : evs)
        if(e.edge == lifecycle_edge::rejected && e.reason == handshake_outcome::reject_unauthorized)
            ++n;
    return n;
}

// A directional secured link: each side carries its own policy + seam; the test reads
// the per-side security/lifecycle events to assert a refusal in EITHER direction.
struct link
{
    inproc_bus<>                            bus;
    inproc_executor<>                       ex{bus};
    inproc_transport<>                      transport{ex, bus};
    plexus::log::null_logger                sink;
    msg_forwarder                           req_messages{sink};
    msg_forwarder                           resp_messages{sink};
    rpc_forwarder                           req_procedures{ex, k_long_timeout, sink};
    rpc_forwarder                           resp_procedures{ex, k_long_timeout, sink};
    plexus::io::peer_context<inproc_policy> req_ctx;
    plexus::io::peer_context<inproc_policy> resp_ctx;
    std::optional<session>                  requester;
    std::optional<session>                  responder;
    security_seam                           req_seam;
    security_seam                           resp_seam;
    std::vector<security_event>             req_security;
    std::vector<security_event>             resp_security;
    std::vector<lifecycle_event>            req_lifecycle;
    std::vector<lifecycle_event>            resp_lifecycle;

    link(const attach_policy *req_policy, const attach_policy *resp_policy, security_seam rseam, security_seam pseam)
            : req_seam(std::move(rseam))
            , resp_seam(std::move(pseam))
    {
        transport.on_accepted(
                [this, resp_policy](std::unique_ptr<inproc_channel<>> ch)
                {
                    resp_ctx.channel   = std::move(ch);
                    resp_ctx.node_name = "requester-node";
                    resp_ctx.peer_id   = make_cfg(0x02, nullptr).self_id;
                    responder.emplace(resp_ctx, ex, make_cfg(0x01, resp_policy), k_long_timeout, resp_messages, resp_procedures, true, sink);
                    responder->set_security_seam(&resp_seam);
                    responder->on_security([this](const security_event &ev) { resp_security.push_back(ev); });
                    responder->on_lifecycle([this](const lifecycle_event &ev) { resp_lifecycle.push_back(ev); });
                    responder->on_install_security([](const security_negotiation &) {});
                    responder->start();
                });
        transport.on_dialed(
                [this, req_policy](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &)
                {
                    req_ctx.channel   = std::move(ch);
                    req_ctx.node_name = "responder-node";
                    req_ctx.peer_id   = make_cfg(0x01, nullptr).self_id;
                    requester.emplace(req_ctx, ex, make_cfg(0x02, req_policy), k_long_timeout, req_messages, req_procedures, false, sink);
                    requester->set_security_seam(&req_seam);
                    requester->on_security([this](const security_event &ev) { req_security.push_back(ev); });
                    requester->on_lifecycle([this](const lifecycle_event &ev) { req_lifecycle.push_back(ev); });
                    requester->on_install_security([](const security_negotiation &) {});
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

}

TEST_CASE("downgrade: a forced cipher/version downgrade is refused with downgrade_refused, dialer "
          "side",
          "[integration][downgrade]")
{
    transcript_policy req_pol; // the dialer refuses the tampered transcript
    admit_policy      resp_pol;
    link              l{&req_pol, &resp_pol, tampered_seam(), honest_seam()};
    l.drive();

    REQUIRE(count_kind(l.req_security, security_kind::downgrade_refused) == 1);
    REQUIRE(count_rejected(l.req_lifecycle) == 1);
    REQUIRE_FALSE(l.requester->is_complete());
}

TEST_CASE("downgrade: a forced cipher/version downgrade is refused with downgrade_refused, "
          "accepter side",
          "[integration][downgrade]")
{
    admit_policy      req_pol;
    transcript_policy resp_pol; // the accepter refuses the tampered transcript
    link              l{&req_pol, &resp_pol, honest_seam(), tampered_seam()};
    l.drive();

    REQUIRE(count_kind(l.resp_security, security_kind::downgrade_refused) == 1);
    REQUIRE(count_rejected(l.resp_lifecycle) == 1);
    REQUIRE_FALSE(l.responder->is_complete());
}

TEST_CASE("downgrade: an honest secured-vs-secured pair completes with NO posture event", "[integration][downgrade]")
{
    admit_policy req_pol;
    admit_policy resp_pol;
    link         l{&req_pol, &resp_pol, honest_seam(), honest_seam()};
    l.drive();

    REQUIRE(l.requester->is_complete());
    REQUIRE(l.responder->is_complete());
    REQUIRE(count_kind(l.req_security, security_kind::posture_mismatch) == 0);
    REQUIRE(count_kind(l.resp_security, security_kind::posture_mismatch) == 0);
}

TEST_CASE("downgrade: a secured-vs-plain posture mismatch is refused with posture_mismatch BOTH ways", "[integration][downgrade]")
{
    admit_policy req_pol;
    admit_policy resp_pol;

    // The accepter is the verifier that receives the first frame, so it is where the
    // mismatch surfaces in either configuration. The two SECTIONs cover BOTH directions
    // of the mismatch (a secured peer met by a plain local, and a plain peer met by a
    // secured local) — both are fail-closed refusals with the distinguishing posture
    // event, never a silent plaintext fallback.
    SECTION("a secured dialer meets a plain accepter")
    {
        link l{&req_pol, nullptr, honest_seam(), security_seam{}};
        l.drive();
        REQUIRE(count_kind(l.resp_security, security_kind::posture_mismatch) == 1);
        REQUIRE_FALSE(l.responder->is_complete());
        REQUIRE_FALSE(l.requester->is_complete());
    }

    SECTION("a plain dialer meets a secured accepter")
    {
        link l{nullptr, &resp_pol, security_seam{}, honest_seam()};
        l.drive();
        REQUIRE(count_kind(l.resp_security, security_kind::posture_mismatch) == 1);
        REQUIRE_FALSE(l.responder->is_complete());
        REQUIRE_FALSE(l.requester->is_complete());
    }
}
