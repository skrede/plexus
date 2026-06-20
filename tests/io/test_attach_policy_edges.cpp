#include "test_attach_policy_common.h"

using namespace attach_policy_fixture;

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
