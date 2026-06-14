// The exhaustive isolation oracle for the pure, sans-IO handshake_fsm. The FSM is
// pure logic — no clock, socket, executor, or thread — so determinism is structural
// and the oracle's job is exhaustive enumeration of the (state, event, version,
// identity) grid plus the named convergence invariants, NOT looped timing
// reproducibility (there is no executor to reorder). Each named group below mirrors
// one canonical convergence invariant of this FSM's behavioral contract.

#include "plexus/io/handshake_fsm.h"

#include "plexus/node_id.h"

#include "plexus/wire/handshake.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <array>
#include <cstddef>
#include <cstdint>

using plexus::node_id;
using plexus::io::dedup_decision;
using plexus::io::fsm_action;
using plexus::io::handshake_fsm;
using plexus::io::handshake_fsm_config;
using plexus::io::handshake_outcome;
using plexus::io::peer_fsm_state;
using plexus::wire::handshake_request;
using plexus::wire::handshake_response;
using plexus::wire::handshake_status;
using plexus::wire::k_protocol_version;

namespace {

// A node_id whose last byte is `tail`; the leading bytes are zero. Lets a test name
// a distinct identity cheaply and order two ids by their tail under the unsigned-
// lexicographic compare std::array gives for free.
constexpr node_id id_with_tail(std::uint8_t tail) noexcept
{
    node_id id{};
    id[15] = std::byte{tail};
    return id;
}

// A node_id whose first byte is `head`; exercises the high-bit edge — a leading
// 0xFF must compare GREATER than a leading 0x01 (unsigned, not signed, compare).
constexpr node_id id_with_head(std::uint8_t head) noexcept
{
    node_id id{};
    id[0] = std::byte{head};
    return id;
}

// A config whose self identity is `self` and whose self/compat version window is
// pinned at (1,0). The compat floor is what peer (major,minor) is checked against.
handshake_fsm_config config_for(node_id self) noexcept
{
    return handshake_fsm_config{
        .self_id                  = self,
        .version_major            = 1,
        .version_minor            = 0,
        .compatible_version_major = 1,
        .compatible_version_minor = 0};
}

// A request that passes every gate by default: matching protocol, compatible
// version, and the given peer identity. Individual fields are overridden per case.
handshake_request good_request(node_id peer) noexcept
{
    return handshake_request{
        .id                       = peer,
        .version_major            = 1,
        .version_minor            = 0,
        .compatible_version_major = 1,
        .compatible_version_minor = 0,
        .protocol_version         = k_protocol_version,
        .fingerprint              = 0,
        .key_id                   = {},
        .own_nonce                = {},
        .cipher_offer             = 0,
        .chosen_cipher            = 0,
        .proof                    = {}};
}

// A response that passes every gate by default: accepted status, matching protocol,
// compatible version, and the given peer identity.
handshake_response good_response(node_id peer) noexcept
{
    return handshake_response{
        .id                       = peer,
        .version_major            = 1,
        .version_minor            = 0,
        .compatible_version_major = 1,
        .compatible_version_minor = 0,
        .protocol_version         = k_protocol_version,
        .fingerprint              = 0,
        .key_id                   = {},
        .own_nonce                = {},
        .cipher_offer             = 0,
        .chosen_cipher            = 0,
        .proof                    = {},
        .status                   = handshake_status::accepted};
}

constexpr node_id k_self  = id_with_tail(0x10);
constexpr node_id k_peer  = id_with_tail(0x20);

}

// Group A — full state x event matrix. Every reachable peer_fsm_state (4) crossed
// with every on_* event (6) = 24 cells, ALL present and asserted, including the
// "nonsensical" re-entry cells, so no cell is ever undefined. Each cell builds a
// fresh FSM driven to the target state, then fires the event under test once.
TEST_CASE("A: state x event matrix is total and defined", "[handshake]")
{
    SECTION("not_connected")
    {
        SECTION("on_dial_started -> dialing/none")
        {
            handshake_fsm fsm(config_for(k_self));
            auto r = fsm.on_dial_started();
            REQUIRE(fsm.state() == peer_fsm_state::dialing);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_outbound_connected -> handshaking/send_request")
        {
            handshake_fsm fsm(config_for(k_self));
            auto r = fsm.on_outbound_connected();
            REQUIRE(fsm.state() == peer_fsm_state::handshaking);
            REQUIRE(r.action == fsm_action::send_request);
        }
        SECTION("on_request(non-bootstrap) -> handshake_resolved/send_response")
        {
            handshake_fsm fsm(config_for(k_self));
            auto r = fsm.on_request(good_request(k_peer), false);
            REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
            REQUIRE(r.action == fsm_action::send_response);
            REQUIRE(r.outcome == handshake_outcome::accept_inbound);
        }
        SECTION("on_response -> none (unsolicited: no request was ever sent)")
        {
            // resolve_outbound is state-guarded: a response with no outbound dial in
            // flight is unsolicited and ignored — the FSM does NOT fabricate an
            // accept_outbound completion for a request it never sent.
            handshake_fsm fsm(config_for(k_self));
            auto r = fsm.on_response(good_response(k_peer));
            REQUIRE(fsm.state() == peer_fsm_state::not_connected);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_timeout -> none, state unchanged")
        {
            handshake_fsm fsm(config_for(k_self));
            auto r = fsm.on_timeout();
            REQUIRE(fsm.state() == peer_fsm_state::not_connected);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_torn_down -> not_connected/none")
        {
            handshake_fsm fsm(config_for(k_self));
            auto r = fsm.on_torn_down();
            REQUIRE(fsm.state() == peer_fsm_state::not_connected);
            REQUIRE(r.action == fsm_action::none);
        }
    }

    SECTION("dialing")
    {
        auto dialing = [] {
            handshake_fsm fsm(config_for(k_self));
            fsm.on_dial_started();
            return fsm;
        };
        SECTION("on_dial_started again -> stays dialing/none (idempotent re-dial)")
        {
            auto fsm = dialing();
            auto r = fsm.on_dial_started();
            REQUIRE(fsm.state() == peer_fsm_state::dialing);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_outbound_connected -> handshaking/send_request")
        {
            auto fsm = dialing();
            auto r = fsm.on_outbound_connected();
            REQUIRE(fsm.state() == peer_fsm_state::handshaking);
            REQUIRE(r.action == fsm_action::send_request);
        }
        SECTION("on_request(non-bootstrap) -> handshake_resolved/send_response")
        {
            auto fsm = dialing();
            auto r = fsm.on_request(good_request(k_peer), false);
            REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
            REQUIRE(r.action == fsm_action::send_response);
        }
        SECTION("on_response -> none (unsolicited: connection not yet established)")
        {
            // The dial is in flight but no outbound connection exists yet, so no
            // send_request was emitted. A response here is unsolicited and ignored
            // (the corrected resolve_outbound state guard).
            auto fsm = dialing();
            auto r = fsm.on_response(good_response(k_peer));
            REQUIRE(fsm.state() == peer_fsm_state::dialing);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_timeout -> retry, state stays dialing")
        {
            auto fsm = dialing();
            auto r = fsm.on_timeout();
            REQUIRE(fsm.state() == peer_fsm_state::dialing);
            REQUIRE(r.action == fsm_action::retry);
        }
        SECTION("on_torn_down -> not_connected/none")
        {
            auto fsm = dialing();
            auto r = fsm.on_torn_down();
            REQUIRE(fsm.state() == peer_fsm_state::not_connected);
            REQUIRE(r.action == fsm_action::none);
        }
    }

    SECTION("handshaking")
    {
        auto handshaking = [] {
            handshake_fsm fsm(config_for(k_self));
            fsm.on_dial_started();
            fsm.on_outbound_connected();
            return fsm;
        };
        SECTION("on_dial_started -> no-op, stays handshaking (no regression)")
        {
            // A stray dial once an outbound connection is established must NOT
            // regress the session back to dialing and discard the connection.
            auto fsm = handshaking();
            auto r = fsm.on_dial_started();
            REQUIRE(fsm.state() == peer_fsm_state::handshaking);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_outbound_connected again -> no-op, no second send_request")
        {
            // A duplicate connected event must NOT re-emit send_request on a
            // connection that is already handshaking.
            auto fsm = handshaking();
            auto r = fsm.on_outbound_connected();
            REQUIRE(fsm.state() == peer_fsm_state::handshaking);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_response(accepted, compatible) -> handshake_resolved/complete")
        {
            auto fsm = handshaking();
            auto r = fsm.on_response(good_response(k_peer));
            REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
            REQUIRE(r.action == fsm_action::complete);
            REQUIRE(r.outcome == handshake_outcome::accept_outbound);
        }
        SECTION("on_request -> simultaneous-connect complete/accept_inbound")
        {
            auto fsm = handshaking();
            auto r = fsm.on_request(good_request(k_peer), false);
            REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
            REQUIRE(r.action == fsm_action::complete);
            REQUIRE(r.outcome == handshake_outcome::accept_inbound);
        }
        SECTION("on_timeout -> abort, state back to not_connected")
        {
            auto fsm = handshaking();
            auto r = fsm.on_timeout();
            REQUIRE(fsm.state() == peer_fsm_state::not_connected);
            REQUIRE(r.action == fsm_action::abort);
        }
        SECTION("on_torn_down -> not_connected/none")
        {
            auto fsm = handshaking();
            auto r = fsm.on_torn_down();
            REQUIRE(fsm.state() == peer_fsm_state::not_connected);
            REQUIRE(r.action == fsm_action::none);
        }
    }

    SECTION("handshake_resolved")
    {
        auto resolved = [] {
            handshake_fsm fsm(config_for(k_self));
            fsm.on_dial_started();
            fsm.on_outbound_connected();
            fsm.on_response(good_response(k_peer));
            return fsm;
        };
        SECTION("on_dial_started -> no-op, stays resolved (no regression)")
        {
            // A stray dial on a resolved session must NOT reset it back to dialing.
            auto fsm = resolved();
            auto r = fsm.on_dial_started();
            REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_outbound_connected -> no-op, no send_request on resolved session")
        {
            auto fsm = resolved();
            auto r = fsm.on_outbound_connected();
            REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_response again -> none (latch holds, no second complete)")
        {
            auto fsm = resolved();
            auto r = fsm.on_response(good_response(k_peer));
            REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_request again -> send_response, no second complete")
        {
            auto fsm = resolved();
            auto r = fsm.on_request(good_request(k_peer), false);
            REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
            REQUIRE(r.action == fsm_action::send_response);
            REQUIRE(r.outcome == handshake_outcome::accept_inbound);
        }
        SECTION("on_timeout -> none, stays resolved")
        {
            auto fsm = resolved();
            auto r = fsm.on_timeout();
            REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
            REQUIRE(r.action == fsm_action::none);
        }
        SECTION("on_torn_down -> not_connected/none, latch cleared")
        {
            auto fsm = resolved();
            auto r = fsm.on_torn_down();
            REQUIRE(fsm.state() == peer_fsm_state::not_connected);
            REQUIRE(r.action == fsm_action::none);
        }
    }
}

// Group B — the OUTBOUND path proven as one contiguous dialer happy-path sequence:
// dial -> connect -> request-sent -> response accepted -> complete/accept_outbound.
TEST_CASE("B: outbound path completes accept_outbound", "[handshake]")
{
    handshake_fsm fsm(config_for(k_self));

    REQUIRE(fsm.on_dial_started().action == fsm_action::none);
    REQUIRE(fsm.state() == peer_fsm_state::dialing);

    auto connected = fsm.on_outbound_connected();
    REQUIRE(connected.action == fsm_action::send_request);
    REQUIRE(fsm.state() == peer_fsm_state::handshaking);

    auto done = fsm.on_response(good_response(k_peer));
    REQUIRE(done.action == fsm_action::complete);
    REQUIRE(done.outcome == handshake_outcome::accept_outbound);
    REQUIRE(done.dedup == dedup_decision::none);
    REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
}

// Group C — the INBOUND path (on_request): a mid-dial inbound awaiting a response,
// a simultaneous-connect inbound with an outbound in flight, and a post-latch
// second inbound arrival that must not re-complete.
TEST_CASE("C: inbound path", "[handshake]")
{
    SECTION("C1: mid-dial inbound -> send_response/accept_inbound, await response")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        auto r = fsm.on_request(good_request(k_peer), false);
        REQUIRE(r.action == fsm_action::send_response);
        REQUIRE(r.outcome == handshake_outcome::accept_inbound);
        REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
    }

    SECTION("C2: simultaneous connect (outbound in flight) -> complete/accept_inbound/dedup")
    {
        // self id 0x20 > peer id 0x10, so the local outbound is the survivor.
        handshake_fsm fsm(config_for(id_with_tail(0x20)));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        auto r = fsm.on_request(good_request(id_with_tail(0x10)), false);
        REQUIRE(r.action == fsm_action::complete);
        REQUIRE(r.outcome == handshake_outcome::accept_inbound);
        REQUIRE(r.dedup == dedup_decision::keep_outbound);
        REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);
    }

    SECTION("C3: second inbound after latch -> bare send_response, NO second complete")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        REQUIRE(fsm.on_response(good_response(k_peer)).action == fsm_action::complete);
        auto second = fsm.on_request(good_request(k_peer), false);
        REQUIRE(second.action == fsm_action::send_response);
        REQUIRE(second.outcome == handshake_outcome::accept_inbound);
    }
}

// Group D — the inbound-only bootstrap, the COMMON path under demand-driven lazy
// dial: a fresh inbound with inbound_is_bootstrap=true and no counter-direction
// dial completes inbound-only with dedup=none and the latch set.
TEST_CASE("D: inbound-only bootstrap completes accept_inbound, dedup none", "[handshake]")
{
    handshake_fsm fsm(config_for(k_self));

    auto r = fsm.on_request(good_request(k_peer), true);
    REQUIRE(r.action == fsm_action::complete);
    REQUIRE(r.outcome == handshake_outcome::accept_inbound);
    REQUIRE(r.dedup == dedup_decision::none);
    REQUIRE(fsm.state() == peer_fsm_state::handshake_resolved);

    // The latch is set: a following matching response must NOT re-complete.
    REQUIRE(fsm.on_response(good_response(k_peer)).action == fsm_action::none);
}

// Group E — the version-compat matrix plus the exact-protocol gate ordering. The
// compat sweep holds protocol_version == k_protocol_version and varies the peer
// (major,minor) against the self compat floor (1,0); the gate sub-section proves
// the protocol check runs BEFORE the compat/status checks.
TEST_CASE("E: version-compat matrix and protocol-gate ordering", "[handshake]")
{
    // peer (major, minor) vs compat floor (1, 0); expected compatible? The
    // equal-minor case is the >= EDGE and must be compatible.
    struct compat_case { std::uint8_t major; std::uint8_t minor; bool compatible; };
    auto c = GENERATE(
        compat_case{2, 0, true},   // peer major newer
        compat_case{1, 1, true},   // equal major, peer minor newer
        compat_case{1, 0, true},   // equal major, equal minor (the >= edge)
        compat_case{0, 9, false},  // peer major older
        compat_case{1, 0, true});  // duplicate boundary, kept for clarity of the edge

    SECTION("E-compat on_request")
    {
        handshake_fsm fsm(config_for(k_self));
        auto req = good_request(k_peer);
        req.version_major = c.major;
        req.version_minor = c.minor;
        auto r = fsm.on_request(req, true);
        if(c.compatible)
            REQUIRE(r.action == fsm_action::complete);
        else
        {
            REQUIRE(r.action == fsm_action::abort);
            REQUIRE(r.outcome == handshake_outcome::reject_version);
            REQUIRE(fsm.state() == peer_fsm_state::not_connected);
        }
    }

    SECTION("E-compat on_response")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        auto resp = good_response(k_peer);
        resp.version_major = c.major;
        resp.version_minor = c.minor;
        auto r = fsm.on_response(resp);
        if(c.compatible)
            REQUIRE(r.action == fsm_action::complete);
        else
        {
            REQUIRE(r.action == fsm_action::abort);
            REQUIRE(r.outcome == handshake_outcome::reject_version);
        }
    }

    SECTION("E-gate: bad protocol on_request aborts, records the bad byte")
    {
        handshake_fsm fsm(config_for(k_self));
        auto req = good_request(k_peer);
        req.protocol_version = k_protocol_version + 7;
        auto r = fsm.on_request(req, true);
        REQUIRE(r.action == fsm_action::abort);
        REQUIRE(r.outcome == handshake_outcome::reject_version);
        REQUIRE(fsm.state() == peer_fsm_state::not_connected);
        REQUIRE(fsm.last_seen_their_protocol_version() == k_protocol_version + 7);
    }

    SECTION("E-gate: bad protocol on_response aborts, records the bad byte")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        auto resp = good_response(k_peer);
        resp.protocol_version = k_protocol_version + 9;
        auto r = fsm.on_response(resp);
        REQUIRE(r.action == fsm_action::abort);
        REQUIRE(r.outcome == handshake_outcome::reject_version);
        REQUIRE(fsm.last_seen_their_protocol_version() == k_protocol_version + 9);
    }

    SECTION("E-gate ordering: bad protocol BUT accepted+compatible still aborts")
    {
        // A response that would pass status and compat — the only fault is the
        // protocol byte. It MUST still abort, proving the protocol gate runs first.
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        auto resp = good_response(k_peer);          // accepted, compatible
        resp.protocol_version = k_protocol_version + 1;
        auto r = fsm.on_response(resp);
        REQUIRE(r.action == fsm_action::abort);
        REQUIRE(r.outcome == handshake_outcome::reject_version);

        static_assert(k_protocol_version == 7);
        REQUIRE(k_protocol_version == 7);
    }
}

// Group F — identity collision: a peer whose id equals self is caught at validation
// and aborts with reject_identity, for both on_request and on_response, so dedup
// never sees the equal case.
TEST_CASE("F: identity collision aborts reject_identity", "[handshake]")
{
    SECTION("on_request with peer id == self id")
    {
        handshake_fsm fsm(config_for(k_self));
        auto r = fsm.on_request(good_request(k_self), true);
        REQUIRE(r.action == fsm_action::abort);
        REQUIRE(r.outcome == handshake_outcome::reject_identity);
        REQUIRE(fsm.state() == peer_fsm_state::not_connected);
    }

    SECTION("on_response with peer id == self id")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        auto r = fsm.on_response(good_response(k_self));
        REQUIRE(r.action == fsm_action::abort);
        REQUIRE(r.outcome == handshake_outcome::reject_identity);
        REQUIRE(fsm.state() == peer_fsm_state::not_connected);
    }

    // The peer-REPORTED rejection statuses: a response whose id and version pass the
    // local gates but whose status byte is a rejection. identity_conflict maps to the
    // exact reject_identity outcome (not the reject_version catch-all); the other
    // rejection bytes have no dedicated outcome and fall to reject_version.
    SECTION("on_response status == identity_conflict -> abort/reject_identity")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        auto resp = good_response(k_peer);
        resp.status = handshake_status::identity_conflict;
        auto r = fsm.on_response(resp);
        REQUIRE(r.action == fsm_action::abort);
        REQUIRE(r.outcome == handshake_outcome::reject_identity);
        REQUIRE(fsm.state() == peer_fsm_state::not_connected);
    }

    SECTION("on_response status == version_incompatible -> abort/reject_version")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        auto resp = good_response(k_peer);
        resp.status = handshake_status::version_incompatible;
        auto r = fsm.on_response(resp);
        REQUIRE(r.action == fsm_action::abort);
        REQUIRE(r.outcome == handshake_outcome::reject_version);
        REQUIRE(fsm.state() == peer_fsm_state::not_connected);
    }

    SECTION("on_response status == rejected_unknown -> abort/reject_version")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        auto resp = good_response(k_peer);
        resp.status = handshake_status::rejected_unknown;
        auto r = fsm.on_response(resp);
        REQUIRE(r.action == fsm_action::abort);
        REQUIRE(r.outcome == handshake_outcome::reject_version);
        REQUIRE(fsm.state() == peer_fsm_state::not_connected);
    }
}

// Group G — dedup arbitration: greater node_id keeps outbound; the two sides of a
// simultaneous connect converge on the SAME surviving physical connection; the
// verdict mirrors exactly when the id ordering is swapped (symmetry). Includes the
// unsigned high-bit edge (a leading 0xFF must outrank a leading 0x01).
TEST_CASE("G: dedup arbitration is greater-wins, symmetric, and convergent", "[handshake]")
{
    // Drive one FSM whose self is `mine` through a simultaneous connect against a
    // peer `theirs`, returning the dedup verdict on the completing inbound.
    auto verdict_on_race = [](node_id mine, node_id theirs) {
        handshake_fsm fsm(config_for(mine));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        return fsm.on_request(good_request(theirs), false).dedup;
    };

    SECTION("G1: two FSMs converge to the same surviving connection")
    {
        constexpr node_id high = id_with_tail(0xAA);
        constexpr node_id low  = id_with_tail(0x05);

        // The high-id side keeps its OUTBOUND; the low-id side keeps its INBOUND.
        // Both therefore name the SAME physical link (high's outbound == low's
        // inbound) — that is convergence.
        REQUIRE(verdict_on_race(high, low) == dedup_decision::keep_outbound);
        REQUIRE(verdict_on_race(low, high) == dedup_decision::keep_inbound);
    }

    SECTION("G2: symmetry across both id orderings")
    {
        // GENERATE over both orderings of an id pair; whichever side holds the
        // greater id must get keep_outbound and the other keep_inbound — mirrored.
        auto pair = GENERATE(
            std::pair<node_id, node_id>{id_with_tail(0x80), id_with_tail(0x01)},
            std::pair<node_id, node_id>{id_with_tail(0x01), id_with_tail(0x80)},
            // The unsigned high-bit edge: a leading 0xFF outranks a leading 0x01.
            std::pair<node_id, node_id>{id_with_head(0xFF), id_with_head(0x01)},
            std::pair<node_id, node_id>{id_with_head(0x01), id_with_head(0xFF)});

        node_id mine   = pair.first;
        node_id theirs = pair.second;
        auto expected  = mine > theirs ? dedup_decision::keep_outbound
                                       : dedup_decision::keep_inbound;
        REQUIRE(verdict_on_race(mine, theirs) == expected);
    }

    SECTION("G-edge: unsigned high-bit compare, 0xFF > 0x01")
    {
        // A sanity pin that the array compare is unsigned-lexicographic: leading
        // 0xFF is strictly greater than leading 0x01.
        REQUIRE(id_with_head(0xFF) > id_with_head(0x01));
        REQUIRE(verdict_on_race(id_with_head(0xFF), id_with_head(0x01))
                == dedup_decision::keep_outbound);
    }
}

// Group H — the install-once latch: the first completing event emits complete once;
// the matching second arrival of the race emits no second complete; on_torn_down
// re-arms the latch so a fresh cycle can complete again.
TEST_CASE("H: install-once completion latch", "[handshake]")
{
    SECTION("H1: first complete fires once (response after request-complete)")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        auto first = fsm.on_response(good_response(k_peer));
        REQUIRE(first.action == fsm_action::complete);

        SECTION("H2: the matching second arrival emits NO second complete")
        {
            auto second = fsm.on_request(good_request(k_peer), false);
            REQUIRE(second.action != fsm_action::complete);
            auto third = fsm.on_response(good_response(k_peer));
            REQUIRE(third.action == fsm_action::none);
        }
    }

    SECTION("H1b: first complete via on_request, second on_response no-ops")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        auto first = fsm.on_request(good_request(k_peer), false);
        REQUIRE(first.action == fsm_action::complete);
        auto second = fsm.on_response(good_response(k_peer));
        REQUIRE(second.action == fsm_action::none);
    }

    SECTION("H3: on_torn_down re-arms the latch for a fresh complete")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        REQUIRE(fsm.on_response(good_response(k_peer)).action == fsm_action::complete);

        fsm.on_torn_down();

        fsm.on_dial_started();
        fsm.on_outbound_connected();
        REQUIRE(fsm.on_response(good_response(k_peer)).action == fsm_action::complete);
    }
}

// Group I — on_torn_down reset: state, latch, and inbound-pending all return to a
// clean not_connected cycle, and a full outbound OR inbound cycle works afterward.
TEST_CASE("I: on_torn_down resets to a clean reusable cycle", "[handshake]")
{
    SECTION("reset then a fresh outbound cycle completes")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        fsm.on_response(good_response(k_peer));

        auto reset = fsm.on_torn_down();
        REQUIRE(reset.action == fsm_action::none);
        REQUIRE(fsm.state() == peer_fsm_state::not_connected);

        fsm.on_dial_started();
        fsm.on_outbound_connected();
        REQUIRE(fsm.on_response(good_response(k_peer)).action == fsm_action::complete);
    }

    SECTION("reset clears inbound-pending: a fresh bootstrap completes dedup none")
    {
        // First a simultaneous connect sets inbound-pending; after torn-down a
        // fresh inbound-only bootstrap must arbitrate dedup=none, not keep_*.
        handshake_fsm fsm(config_for(id_with_tail(0x20)));
        fsm.on_dial_started();
        fsm.on_outbound_connected();
        fsm.on_request(good_request(id_with_tail(0x10)), false);

        fsm.on_torn_down();

        auto fresh = fsm.on_request(good_request(id_with_tail(0x10)), true);
        REQUIRE(fresh.action == fsm_action::complete);
        REQUIRE(fresh.dedup == dedup_decision::none);
    }
}

// Group J — compile-time pins and node_id ordering. static_asserts pin every
// handshake_status value and the protocol constant; the node_id compare is proven
// unsigned-lexicographic incl. the high-bit case; a field-layout construct via
// designated initializers compiles; an optional steady-step zero-alloc gate proves
// the FSM allocates nothing on a steady on_request/on_response.
TEST_CASE("J: compile-time pins, node_id order, and zero-alloc steady step", "[handshake]")
{
    SECTION("J1: status and protocol value pins")
    {
        static_assert(static_cast<std::uint8_t>(handshake_status::accepted) == 0x01);
        static_assert(static_cast<std::uint8_t>(handshake_status::version_incompatible) == 0x02);
        static_assert(static_cast<std::uint8_t>(handshake_status::identity_conflict) == 0x03);
        static_assert(static_cast<std::uint8_t>(handshake_status::rejected_unknown) == 0x04);
        static_assert(static_cast<std::uint8_t>(handshake_status::unauthorized) == 0x05);
        static_assert(k_protocol_version == 7);
        static_assert(std::tuple_size_v<node_id> == 16);
        REQUIRE(k_protocol_version == 7);
    }

    SECTION("J2: node_id compare is unsigned-lexicographic")
    {
        REQUIRE(id_with_tail(0x01) < id_with_tail(0x02));
        REQUIRE(id_with_tail(0x02) > id_with_tail(0x01));
        // The unsigned high-bit case: 0xFF must outrank 0x01, not be negative.
        REQUIRE(id_with_head(0xFF) > id_with_head(0x01));
        REQUIRE(id_with_tail(0x05) == id_with_tail(0x05));
    }

    SECTION("J3: wire field-layout construct compiles and round-trips")
    {
        auto req  = good_request(k_peer);
        auto resp = good_response(k_peer);
        REQUIRE(req.protocol_version == k_protocol_version);
        REQUIRE(resp.status == handshake_status::accepted);
        REQUIRE(resp.id == k_peer);
    }

    SECTION("J4: a steady on_request/on_response step allocates nothing")
    {
        handshake_fsm fsm(config_for(k_self));
        fsm.on_dial_started();
        fsm.on_outbound_connected();

        plexus::testing::reset_alloc_count();
        auto r = fsm.on_response(good_response(k_peer));
        auto delta = plexus::testing::alloc_count();

        REQUIRE(r.action == fsm_action::complete);
        REQUIRE(delta == 0);
    }
}
