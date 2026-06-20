#include "test_attach_policy_common.h"

using namespace attach_policy_fixture;

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
