// The security attach-gate bridge oracle: two peer_sessions over the inproc backend
// drive the end-to-end attach surface this wave wires — the dedicated security_event,
// the lifecycle rejected edge, the AEAD install hook on a successful attach, the
// stream-tamper teardown, and the auth-only+datagram posture refusal. No OpenSSL: the
// transcript seam is a deterministic fake and the attach_policy is a test double, so
// the bridge logic is proven in the plexus::plexus-only core (the litmus that the
// decorator/EVP instantiation is genuinely behind the type-erased seam).

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

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
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
using plexus::io::security::attach_prover;
using plexus::io::security::psk_keystore_policy;
using plexus::io::security::keyed_psk;
using plexus::io::security::hmac_fn;
using plexus::io::security::rand_fn;
using plexus::io::security::k_key_id_len;
using session = plexus::io::peer_session<inproc_policy>;
using msg_forwarder = plexus::io::message_forwarder<inproc_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<inproc_policy>;

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);

handshake_fsm_config make_cfg(std::uint8_t id_seed, const attach_policy *policy = nullptr)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                .compatible_version_major = 1, .compatible_version_minor = 0,
                                .local_fingerprint = {}, .attach_policy = policy};
}

// A test attach_policy that accepts or refuses by a fixed verdict — the gate mechanism
// is exercised without any crypto. The reject leg drives reject_unauthorized.
struct verdict_policy final : public attach_policy
{
    bool admit{true};
    [[nodiscard]] bool decide(const attach_facts &) const noexcept override { return admit; }
};

// A deterministic transcript fold (NOT a hash — the bridge only needs determinism over
// its input to bind the negotiation). Engaged() is true, so the seam is the secured
// posture.
security_seam fake_seam()
{
    security_seam s;
    s.transcript = [](std::span<const std::byte> in, std::span<std::byte, 32> out) {
        for(std::size_t i = 0; i < out.size(); ++i)
        {
            std::byte acc{0x5a};
            for(std::size_t k = 0; k < in.size(); ++k)
                acc ^= in[k] ^ static_cast<std::byte>(k + i);
            out[i] = acc;
        }
        return true;
    };
    return s;
}

// A deterministic keyed MAC over key||msg (a real keyed mixing function, NOT an identity
// stub — every key/msg bit feeds every output byte, so a wrong key or a tampered input
// yields a different MAC). The proof verification needs a genuine keyed dependence; this
// mirrors the attach_policy oracle's fake_hmac so the prover and the policy agree.
hmac_fn real_keyed_hmac()
{
    return [](std::span<const std::byte> key, std::span<const std::byte> msg, std::span<std::byte> out)
    {
        if(out.size() != 32)
            return false;
        for(std::size_t i = 0; i < out.size(); ++i)
        {
            unsigned acc = 0x811c9dc5u + static_cast<unsigned>(i);
            for(std::size_t k = 0; k < key.size(); ++k)
                acc = (acc ^ std::to_integer<unsigned>(key[k])) * 0x01000193u + static_cast<unsigned>(k);
            for(std::size_t m = 0; m < msg.size(); ++m)
                acc = (acc ^ std::to_integer<unsigned>(msg[m])) * 0x01000193u + static_cast<unsigned>(m + i);
            out[i] = static_cast<std::byte>(acc & 0xffu);
        }
        return true;
    };
}

// A counter-seeded entropy seam: each fill advances a per-functor counter so two distinct
// sessions mint DISTINCT own_nonces (the freshness property under test) while staying
// fully reproducible across runs. seed separates the two ends so their nonces never
// collide by construction.
rand_fn counter_rand(std::uint8_t seed)
{
    auto counter = std::make_shared<std::uint8_t>(0);
    return [seed, counter](std::span<std::byte> out)
    {
        for(auto &b : out)
            b = static_cast<std::byte>((seed << 4) ^ (*counter)++);
        return true;
    };
}

std::array<std::byte, k_key_id_len> psk_key_id(std::uint8_t v)
{
    std::array<std::byte, k_key_id_len> id{};
    id.fill(std::byte{v});
    return id;
}

std::vector<std::byte> psk_material(std::uint8_t seed, std::size_t len = 16)
{
    std::vector<std::byte> m(len);
    for(std::size_t i = 0; i < len; ++i)
        m[i] = static_cast<std::byte>((seed + i) & 0xff);
    return m;
}

// A two-node inproc link with selectable per-node attach policy + seam + the security
// observer wiring. Mirrors the peer_session inproc rig.
struct link
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};

    msg_forwarder req_messages{ex};
    msg_forwarder resp_messages{ex};
    rpc_forwarder req_procedures{ex, k_long_timeout};
    rpc_forwarder resp_procedures{ex, k_long_timeout};

    plexus::io::peer_context<inproc_policy> req_ctx;
    plexus::io::peer_context<inproc_policy> resp_ctx;
    std::optional<session> requester;
    std::optional<session> responder;

    security_seam req_seam;
    security_seam resp_seam;

    std::vector<security_event> req_security;
    std::vector<security_event> resp_security;
    std::vector<lifecycle_event> req_lifecycle;
    std::vector<lifecycle_event> resp_lifecycle;
    int req_installs{0};
    int resp_installs{0};

    // wire_install gates whether the AEAD install hook is set: a secured posture with NO
    // install hook is the auth-only configuration (admission without AEAD).
    link(const attach_policy *req_policy, const attach_policy *resp_policy,
         bool secured, bool wire_install = true, std::chrono::nanoseconds timeout = k_long_timeout)
    {
        if(secured)
        {
            req_seam = fake_seam();
            resp_seam = fake_seam();
        }
        const bool install = wire_install;
        transport.on_accepted([this, resp_policy, timeout, install](std::unique_ptr<inproc_channel<>> ch) {
            resp_ctx.channel = std::move(ch);
            resp_ctx.node_name = "requester-node";
            resp_ctx.peer_id = make_cfg(0x02).self_id;
            responder.emplace(resp_ctx, ex, make_cfg(0x01, resp_policy), timeout,
                              resp_messages, resp_procedures, true);
            responder->set_security_seam(&resp_seam);
            responder->on_security([this](const security_event &ev) { resp_security.push_back(ev); });
            responder->on_lifecycle([this](const lifecycle_event &ev) { resp_lifecycle.push_back(ev); });
            if(install)
                responder->on_install_security([this](const security_negotiation &) { ++resp_installs; });
            responder->start();
        });
        transport.on_dialed([this, req_policy, timeout, install](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &) {
            req_ctx.channel = std::move(ch);
            req_ctx.node_name = "responder-node";
            req_ctx.peer_id = make_cfg(0x01).self_id;
            requester.emplace(req_ctx, ex, make_cfg(0x02, req_policy), timeout,
                              req_messages, req_procedures, false);
            requester->set_security_seam(&req_seam);
            requester->on_security([this](const security_event &ev) { req_security.push_back(ev); });
            requester->on_lifecycle([this](const lifecycle_event &ev) { req_lifecycle.push_back(ev); });
            if(install)
                requester->on_install_security([this](const security_negotiation &) { ++req_installs; });
            requester->start();
        });

        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive() { ex.drain(); }
};

// The PSK end-to-end rig: drives the REAL psk_keystore_policy on the security decision
// path (no verdict_policy double). The dialer's policy verifies the responder's wire proof
// on its on_response; the responder's prover stamps the proof; both ends mint fresh nonces
// from a counter-seeded entropy seam. A secured seam is engaged on both ends (the AEAD
// posture the attach gate runs under), with the install hook wired so an admit completes.
struct psk_link
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};

    msg_forwarder req_messages{ex};
    msg_forwarder resp_messages{ex};
    rpc_forwarder req_procedures{ex, k_long_timeout};
    rpc_forwarder resp_procedures{ex, k_long_timeout};

    plexus::io::peer_context<inproc_policy> req_ctx;
    plexus::io::peer_context<inproc_policy> resp_ctx;
    std::optional<session> requester;
    std::optional<session> responder;

    security_seam req_seam;
    security_seam resp_seam;

    std::vector<security_event> req_security;
    std::vector<security_event> resp_security;
    std::vector<lifecycle_event> req_lifecycle;
    std::vector<lifecycle_event> resp_lifecycle;
    int req_installs{0};
    int resp_installs{0};

    // dialer_keystore: the keyed_psk set the DIALER's policy verifies the responder's proof
    // against (an empty/wrong set drives the refuse legs). responder_key: the keyed_psk the
    // responder PROVES under (its own material + the key_id it stamps).
    psk_link(std::vector<keyed_psk> dialer_keystore, keyed_psk responder_key)
    {
        req_seam = fake_seam();
        resp_seam = fake_seam();

        transport.on_accepted([this, responder_key](std::unique_ptr<inproc_channel<>> ch) mutable {
            resp_ctx.channel = std::move(ch);
            resp_ctx.node_name = "requester-node";
            resp_ctx.peer_id = make_cfg(0x02).self_id;
            // The responder admits the version/identity-valid request (no verifiable proof
            // in flight one) — accept_any on its on_request leg — and stamps ITS proof on
            // the response via the prover.
            responder.emplace(resp_ctx, ex, make_cfg(0x01, &m_accept_any), k_long_timeout,
                              resp_messages, resp_procedures, true);
            responder->set_security_seam(&resp_seam);
            responder->set_attach_entropy(counter_rand(0x0a));
            responder->set_attach_prover(attach_prover{responder_key, real_keyed_hmac()});
            responder->on_security([this](const security_event &ev) { resp_security.push_back(ev); });
            responder->on_lifecycle([this](const lifecycle_event &ev) { resp_lifecycle.push_back(ev); });
            responder->on_install_security([this](const security_negotiation &) { ++resp_installs; });
            responder->start();
        });
        transport.on_dialed([this, dialer_keystore](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &) mutable {
            req_ctx.channel = std::move(ch);
            req_ctx.node_name = "responder-node";
            req_ctx.peer_id = make_cfg(0x01).self_id;
            m_dialer_policy.emplace(std::move(dialer_keystore), real_keyed_hmac());
            requester.emplace(req_ctx, ex, make_cfg(0x02, &*m_dialer_policy), k_long_timeout,
                              req_messages, req_procedures, false);
            requester->set_security_seam(&req_seam);
            requester->set_attach_entropy(counter_rand(0x0b));
            requester->on_security([this](const security_event &ev) { req_security.push_back(ev); });
            requester->on_lifecycle([this](const lifecycle_event &ev) { req_lifecycle.push_back(ev); });
            requester->on_install_security([this](const security_negotiation &) { ++req_installs; });
            requester->start();
        });

        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive() { ex.drain(); }

private:
    plexus::io::security::accept_any m_accept_any;
    std::optional<psk_keystore_policy> m_dialer_policy;
};

int count_kind(const std::vector<security_event> &evs, security_kind k)
{
    int n = 0;
    for(const auto &e : evs)
        if(e.kind == k)
            ++n;
    return n;
}

int count_rejected(const std::vector<lifecycle_event> &evs, handshake_outcome reason)
{
    int n = 0;
    for(const auto &e : evs)
        if(e.edge == lifecycle_edge::rejected && e.reason == reason)
            ++n;
    return n;
}

}

TEST_CASE("io.security_attach a null policy proceeds with no security event (accept-any unchanged)",
          "[io][security_attach]")
{
    link l{nullptr, nullptr, /*secured=*/false};
    l.drive();

    REQUIRE(l.requester->is_complete());
    REQUIRE(l.responder->is_complete());
    REQUIRE(l.req_security.empty());
    REQUIRE(l.resp_security.empty());
    REQUIRE(l.req_installs == 0);
    REQUIRE(l.resp_installs == 0);
}

TEST_CASE("io.security_attach an unauthorized attach fires security_event(unauthorized_attach) AND the lifecycle rejected edge",
          "[io][security_attach]")
{
    verdict_policy reject;
    reject.admit = false;
    // The responder refuses the dialer's request → the dialer's session is the one that
    // attached; the responder is the verifier that fires the refusal.
    link l{nullptr, &reject, /*secured=*/false};
    l.drive();

    REQUIRE(count_kind(l.resp_security, security_kind::unauthorized_attach) == 1);
    REQUIRE(count_rejected(l.resp_lifecycle, handshake_outcome::reject_unauthorized) == 1);
    REQUIRE_FALSE(l.responder->is_complete());
}

TEST_CASE("io.security_attach an authorized secured attach installs the AEAD decorator once, no event",
          "[io][security_attach]")
{
    verdict_policy admit;
    admit.admit = true;
    link l{&admit, &admit, /*secured=*/true};
    l.drive();

    REQUIRE(l.requester->is_complete());
    REQUIRE(l.responder->is_complete());
    // Both ends attached over a plaintext (inproc) network channel under an engaged seam
    // → the decorator install hook fired exactly once per side, with no refusal event.
    REQUIRE(l.req_installs == 1);
    REQUIRE(l.resp_installs == 1);
    REQUIRE(l.req_security.empty());
    REQUIRE(l.resp_security.empty());
    // The authenticated host identity latches from the verified facts (never a wire claim).
    REQUIRE(l.requester->authenticated_host_identity().has_value());
    REQUIRE(l.responder->authenticated_host_identity().has_value());
}

TEST_CASE("io.security_attach a stream tamper (the channel's on_protocol_close on a secured session) fires stream_tamper_teardown",
          "[io][security_attach]")
{
    verdict_policy admit;
    admit.admit = true;
    link l{&admit, &admit, /*secured=*/true};
    l.drive();
    REQUIRE(l.requester->is_complete());

    // Simulate the installed decorator's tamper teardown: the channel surfaces
    // on_protocol_close (a bad AEAD tag on an ordered stream). The secured session fires
    // the dedicated stream_tamper_teardown event and tears down.
    l.req_ctx.channel->deliver_protocol_close(plexus::wire::close_cause::invalid_magic);
    l.drive();

    REQUIRE(count_kind(l.req_security, security_kind::stream_tamper_teardown) == 1);
}

// A handshake_request frame from a matched peer, carrying a non-zero chosen_cipher so
// the posture is "offered AEAD" (not the bare no-offer case). Fed straight to a
// bootstrap responder's on_receive.
std::vector<std::byte> matched_request(std::uint8_t peer_seed)
{
    plexus::node_id peer{};
    peer[0] = std::byte{peer_seed};
    plexus::wire::handshake_request req{.id = peer, .version_major = 1, .version_minor = 0,
        .compatible_version_major = 1, .compatible_version_minor = 0,
        .protocol_version = plexus::wire::k_protocol_version, .fingerprint = 0,
        .key_id = {}, .own_nonce = {}, .cipher_offer = 0x01, .chosen_cipher = 0x01, .proof = {}};
    auto payload = plexus::wire::encode_handshake_request(req);
    plexus::wire::frame_header hdr{.type = plexus::wire::msg_type::handshake_req, .flags = 0,
        .session_id = 0, .timestamp_ns = 0, .payload_len = payload.size()};
    return plexus::wire::encode_frame(hdr, payload);
}

TEST_CASE("io.security_attach an auth-only + datagram configuration is refused (posture_mismatch), never silently proceeded",
          "[io][security_attach]")
{
    verdict_policy admit;
    admit.admit = true;

    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    msg_forwarder messages{ex};
    rpc_forwarder procedures{ex, k_long_timeout};

    plexus::io::peer_context<inproc_policy> ctx;
    ctx.channel = std::make_unique<inproc_channel<>>(ex);
    ctx.channel->connect_to({"udp", "peer"});   // a plaintext datagram channel
    ctx.node_name = "peer-node";
    ctx.peer_id = make_cfg(0x07).self_id;

    security_seam seam = fake_seam();            // secured posture (attach engaged)
    std::vector<security_event> security;
    std::vector<lifecycle_event> lifecycle;

    // A bootstrap responder so a single request completes the attach; the secured seam
    // is engaged but NO install hook is wired — the auth-only datagram configuration.
    session responder{ctx, ex, make_cfg(0x08, &admit), k_long_timeout, messages, procedures, true};
    responder.set_security_seam(&seam);
    responder.on_security([&](const security_event &ev) { security.push_back(ev); });
    responder.on_lifecycle([&](const lifecycle_event &ev) { lifecycle.push_back(ev); });
    responder.start();

    responder.on_receive(matched_request(0x07));
    ex.drain();

    REQUIRE(count_kind(security, security_kind::posture_mismatch) == 1);
    REQUIRE(count_rejected(lifecycle, handshake_outcome::reject_unauthorized) == 1);
    REQUIRE_FALSE(responder.is_complete());   // fail-closed: no silent plaintext fallback
}

TEST_CASE("io.security_attach the REAL psk_keystore_policy admits a matching-key attach end to end",
          "[io][security_attach]")
{
    const auto material = psk_material(0xA0);
    const keyed_psk key{psk_key_id(0x01), material};

    // Both ends hold key_id 0x01 with the SAME material: the responder proves under it on
    // the response, the dialer's REAL psk_keystore_policy looks it up and verifies the proof.
    psk_link l{/*dialer_keystore=*/{key}, /*responder_key=*/key};
    l.drive();

    REQUIRE(l.requester->is_complete());
    REQUIRE(l.responder->is_complete());
    // The decision ran on the real policy over a real wire proof — admitted, identity
    // latched, the install hook fired once per side, and no refusal event.
    REQUIRE(l.requester->authenticated_host_identity().has_value());
    REQUIRE(l.responder->authenticated_host_identity().has_value());
    REQUIRE(l.req_installs == 1);
    REQUIRE(l.resp_installs == 1);
    REQUIRE(l.req_security.empty());
    REQUIRE(l.resp_security.empty());
}

TEST_CASE("io.security_attach the REAL psk_keystore_policy refuses a wrong-key attach end to end",
          "[io][security_attach]")
{
    // The responder proves under material M1; the dialer's keystore holds key_id 0x01 with
    // a DIFFERENT material M2 — the recomputed MAC differs, so the proof fails ct_equal.
    const keyed_psk dialer_key{psk_key_id(0x01), psk_material(0xA0)};
    const keyed_psk responder_key{psk_key_id(0x01), psk_material(0xB0)};

    psk_link l{/*dialer_keystore=*/{dialer_key}, /*responder_key=*/responder_key};
    l.drive();

    // The dialer (the verifier on the response leg) refuses with reject_unauthorized + the
    // lifecycle rejected edge. The dedicated security event on a SECURED pair is
    // downgrade_refused (a secured-pair gate refusal — a forced/failed transcript-bound
    // proof — is classified distinctly from a plain no-posture unauthorized_attach; the
    // posture mismatch is caught earlier). The reject_unauthorized verdict is the real
    // policy's, not a double's.
    REQUIRE(count_kind(l.req_security, security_kind::downgrade_refused) == 1);
    REQUIRE(count_rejected(l.req_lifecycle, handshake_outcome::reject_unauthorized) == 1);
    REQUIRE_FALSE(l.requester->is_complete());
}

TEST_CASE("io.security_attach the REAL psk_keystore_policy refuses a removed-key attach end to end",
          "[io][security_attach]")
{
    // The responder proves under key_id 0x01, but the dialer's keystore omits it entirely
    // (a rotated-out / removed key): the lookup misses and the attach is refused without
    // dereferencing — the fail-closed removed-key path.
    const keyed_psk responder_key{psk_key_id(0x01), psk_material(0xA0)};

    psk_link l{/*dialer_keystore=*/{}, /*responder_key=*/responder_key};
    l.drive();

    // A removed key misses the lookup: the real policy refuses without dereferencing.
    // The secured-pair gate refusal carries downgrade_refused + the reject_unauthorized
    // lifecycle edge (same classification as the wrong-key path).
    REQUIRE(count_kind(l.req_security, security_kind::downgrade_refused) == 1);
    REQUIRE(count_rejected(l.req_lifecycle, handshake_outcome::reject_unauthorized) == 1);
    REQUIRE_FALSE(l.requester->is_complete());
}
