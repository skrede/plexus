// The verify_policy oracle: a pure sans-OpenSSL drive of the accept/reject decision over a
// cert_facts VALUE (no X509, no DER parse, no backend link — plexus::plexus only). It proves
// the spki_pin_policy contract: a pin hit accepts, a pin miss rejects, and an EMPTY allowlist
// rejects ANY facts (fail-closed — the prior-art-inverting default).

#include "plexus/io/security/verify_policy.h"
#include "plexus/io/security/cert_facts.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <vector>
#include <cstddef>

using plexus::io::security::cert_facts;
using plexus::io::security::spki_pin_policy;

namespace {

std::array<std::byte, 32> digest_of(int seed)
{
    std::array<std::byte, 32> d{};
    for(std::size_t i = 0; i < d.size(); ++i)
        d[i] = static_cast<std::byte>((seed + static_cast<int>(i)) & 0xff);
    return d;
}

cert_facts facts_with(const std::array<std::byte, 32> &spki)
{
    cert_facts f;
    f.spki_sha256 = spki;
    return f;
}

}

TEST_CASE("io.verify_policy spki_pin_policy accepts a pinned digest, rejects an unpinned one", "[io][verify_policy]")
{
    spki_pin_policy policy{{digest_of(1), digest_of(2), digest_of(3)}};

    REQUIRE(policy.decide(facts_with(digest_of(1)))); // pin hit
    REQUIRE(policy.decide(facts_with(digest_of(2)))); // multi-pin set hit
    REQUIRE(policy.decide(facts_with(digest_of(3))));

    REQUIRE_FALSE(policy.decide(facts_with(digest_of(9)))); // pin miss rejects
}

TEST_CASE("io.verify_policy empty allowlist rejects any facts (fail-closed)", "[io][verify_policy]")
{
    spki_pin_policy policy{{}};

    REQUIRE_FALSE(policy.decide(facts_with(digest_of(1))));
    REQUIRE_FALSE(policy.decide(facts_with(digest_of(42))));
    REQUIRE_FALSE(policy.decide(cert_facts{})); // even an all-zero / preverify_ok=false cert
}
