// The attach_policy oracle: a pure sans-OpenSSL drive of the PSK attach-admission
// decision over INJECTED fake hmac_fn (no OpenSSL, no backend link — plexus::plexus
// only; the litmus proof the decision needs no crypto lib). It proves: a matching
// (key_id, recomputed-proof) admits; a wrong proof refuses; an unknown key_id
// refuses without dereferencing; dual single-key keystores each admit their peer
// (rotation); a proof computed under one role presented under the other refuses
// (reflection); accept_any admits all; the keystore ctor throws on short material;
// and ct_equal rejects on length mismatch and is order-independent.

#include "plexus/io/security/attach_policy.h"
#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/ct_equal.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <utility>

using plexus::io::security::accept_any;
using plexus::io::security::attach_facts;
using plexus::io::security::attach_role;
using plexus::io::security::ct_equal;
using plexus::io::security::hmac_fn;
using plexus::io::security::keyed_psk;
using plexus::io::security::k_key_id_len;
using plexus::io::security::psk_keystore_policy;

namespace {

// A deterministic position-weighted multiply-accumulate over key||msg (a stand-in for
// HMAC; the only property the decision needs is determinism over its inputs). No
// OpenSSL. Mirrors the cookie_secret oracle's fake_hmac.
hmac_fn fake_hmac()
{
    return [](std::span<const std::byte> key, std::span<const std::byte> msg,
              std::span<std::byte> out)
    {
        if(out.size() != 32)
            return false;
        for(std::size_t i = 0; i < out.size(); ++i)
        {
            unsigned acc = 0x811c9dc5u + static_cast<unsigned>(i);
            for(std::size_t k = 0; k < key.size(); ++k)
                acc = (acc ^ std::to_integer<unsigned>(key[k])) * 0x01000193u +
                        static_cast<unsigned>(k);
            for(std::size_t m = 0; m < msg.size(); ++m)
                acc = (acc ^ std::to_integer<unsigned>(msg[m])) * 0x01000193u +
                        static_cast<unsigned>(m + i);
            out[i] = static_cast<std::byte>(acc & 0xffu);
        }
        return true;
    };
}

std::array<std::byte, k_key_id_len> key_id_of(std::uint8_t v)
{
    std::array<std::byte, k_key_id_len> id{};
    id.fill(std::byte{v});
    return id;
}

std::vector<std::byte> material_of(std::uint8_t seed, std::size_t len = 16)
{
    std::vector<std::byte> m(len);
    for(std::size_t i = 0; i < len; ++i)
        m[i] = static_cast<std::byte>((seed + i) & 0xff);
    return m;
}

attach_facts facts_for(std::uint8_t key_seed, attach_role role)
{
    attach_facts f;
    f.key_id = key_id_of(key_seed);
    f.initiator_id.fill(std::byte{0x10});
    f.responder_id.fill(std::byte{0x20});
    f.peer_nonce.fill(std::byte{0x30});
    f.own_nonce.fill(std::byte{0x40});
    f.transcript_digest.fill(std::byte{0x50});
    f.role = role;
    return f;
}

// Recompute the canonical proof for `facts` under `material` via the same fake HMAC
// the policy uses, so a facts value can be presented with a VALID proof. The proof
// input ordering is the policy's contract; this mirror keeps it in one place.
std::array<std::byte, 32> proof_for(std::span<const std::byte> material, const attach_facts &f)
{
    static constexpr std::array<std::byte, 13> label{
            std::byte{'p'}, std::byte{'l'}, std::byte{'e'}, std::byte{'x'}, std::byte{'u'},
            std::byte{'s'}, std::byte{'-'}, std::byte{'a'}, std::byte{'t'}, std::byte{'t'},
            std::byte{'a'}, std::byte{'c'}, std::byte{'h'}};
    std::vector<std::byte> msg;
    msg.insert(msg.end(), label.begin(), label.end());
    msg.push_back(static_cast<std::byte>(f.role));
    msg.insert(msg.end(), f.initiator_id.begin(), f.initiator_id.end());
    msg.insert(msg.end(), f.responder_id.begin(), f.responder_id.end());
    msg.insert(msg.end(), f.peer_nonce.begin(), f.peer_nonce.end());
    msg.insert(msg.end(), f.own_nonce.begin(), f.own_nonce.end());
    msg.insert(msg.end(), f.transcript_digest.begin(), f.transcript_digest.end());
    std::array<std::byte, 32> out{};
    fake_hmac()(material, msg, out);
    return out;
}

}

TEST_CASE("io.attach_policy admits a matching key-id with a valid recomputed proof",
          "[io][attach_policy]")
{
    const auto          material = material_of(0xA0);
    psk_keystore_policy policy{{{key_id_of(0x01), material}}, fake_hmac()};

    auto       facts = facts_for(0x01, attach_role::initiator);
    const auto proof = proof_for(material, facts);
    facts.proof      = proof;

    REQUIRE(policy.decide(facts));
}

TEST_CASE("io.attach_policy refuses a wrong proof", "[io][attach_policy]")
{
    const auto          material = material_of(0xA0);
    psk_keystore_policy policy{{{key_id_of(0x01), material}}, fake_hmac()};

    auto facts = facts_for(0x01, attach_role::initiator);
    auto proof = proof_for(material, facts);
    proof[0] ^= std::byte{0xff}; // a one-byte-off proof
    facts.proof = proof;

    REQUIRE_FALSE(policy.decide(facts));
}

TEST_CASE("io.attach_policy refuses an unknown / removed key-id", "[io][attach_policy]")
{
    const auto          material = material_of(0xA0);
    psk_keystore_policy policy{{{key_id_of(0x01), material}}, fake_hmac()};

    auto       facts = facts_for(0x09, attach_role::initiator); // no key 0x09 in the store
    const auto proof = proof_for(material, facts);
    facts.proof      = proof;

    REQUIRE_FALSE(policy.decide(facts));
}

TEST_CASE("io.attach_policy dual single-key keystores each admit their own peer (rotation)",
          "[io][attach_policy]")
{
    const auto          old_key = material_of(0xA0);
    const auto          new_key = material_of(0xB0);
    psk_keystore_policy policy{{{key_id_of(0x01), old_key}, {key_id_of(0x02), new_key}},
                               fake_hmac()};

    auto old_facts  = facts_for(0x01, attach_role::initiator);
    auto old_proof  = proof_for(old_key, old_facts);
    old_facts.proof = old_proof;

    auto new_facts  = facts_for(0x02, attach_role::responder);
    auto new_proof  = proof_for(new_key, new_facts);
    new_facts.proof = new_proof;

    REQUIRE(policy.decide(old_facts));
    REQUIRE(policy.decide(new_facts));

    // A proof minted under the old key but presented with the new key-id refuses.
    auto crossed  = facts_for(0x02, attach_role::initiator);
    crossed.proof = old_proof;
    REQUIRE_FALSE(policy.decide(crossed));
}

TEST_CASE("io.attach_policy refuses a proof presented under the other role (reflection)",
          "[io][attach_policy]")
{
    const auto          material = material_of(0xA0);
    psk_keystore_policy policy{{{key_id_of(0x01), material}}, fake_hmac()};

    // Compute the proof for the initiator direction, then present it as a responder.
    auto       as_initiator = facts_for(0x01, attach_role::initiator);
    const auto reflected    = proof_for(material, as_initiator);

    auto as_responder  = facts_for(0x01, attach_role::responder);
    as_responder.proof = reflected;

    REQUIRE_FALSE(policy.decide(as_responder));
}

TEST_CASE("io.attach_policy keystore ctor throws on material below the minimum length",
          "[io][attach_policy]")
{
    REQUIRE_THROWS(
            [&]
            {
                psk_keystore_policy bad{{{key_id_of(0x01), material_of(0xA0, 15)}}, fake_hmac()};
            }());
    REQUIRE_NOTHROW(
            [&]
            { psk_keystore_policy ok{{{key_id_of(0x01), material_of(0xA0, 16)}}, fake_hmac()}; }());
}

TEST_CASE("io.attach_policy empty keystore refuses everything (fail-closed)", "[io][attach_policy]")
{
    psk_keystore_policy       policy{{}, fake_hmac()};
    auto                      facts = facts_for(0x01, attach_role::initiator);
    std::array<std::byte, 32> proof{};
    facts.proof = proof;
    REQUIRE_FALSE(policy.decide(facts));
}

TEST_CASE("io.attach_policy accept_any admits every facts value", "[io][attach_policy]")
{
    accept_any policy;
    REQUIRE(policy.decide(facts_for(0x01, attach_role::initiator)));
    REQUIRE(policy.decide(attach_facts{}));
}

TEST_CASE("io.attach_policy ct_equal rejects a length mismatch and is order-independent",
          "[io][attach_policy]")
{
    const std::array<std::byte, 4> a{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    const std::array<std::byte, 4> a_copy = a;
    const std::array<std::byte, 3> shorter{std::byte{1}, std::byte{2}, std::byte{3}};

    REQUIRE(ct_equal(a, a_copy));
    REQUIRE_FALSE(ct_equal(a, shorter));

    for(std::size_t i = 0; i < a.size(); ++i)
    {
        auto diff = a;
        diff[i] ^= std::byte{0xff};
        REQUIRE_FALSE(ct_equal(a, diff)); // a difference in ANY byte rejects
    }
}
