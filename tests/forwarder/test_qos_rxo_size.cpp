#include "test_qos_rxo_common.h"

#include <array>

using namespace qos_rxo_fixture;

TEST_CASE("qos rxo: max-message-bytes relation")
{
    // The publisher's effective-max must fit the subscriber's requested ceiling
    // (offered <= requested). 0 requested is "unset" and always compatible; the
    // offered side is already the resolved effective-max at the call site.
    REQUIRE(max_message_bytes_compatible(4u * 1024u * 1024u, 0, k_unlimited_message_bytes)); // unset request
    REQUIRE(max_message_bytes_compatible(1u * 1024u * 1024u, 4u * 1024u * 1024u, k_unlimited_message_bytes));
    REQUIRE(max_message_bytes_compatible(4u * 1024u * 1024u, 4u * 1024u * 1024u, k_unlimited_message_bytes));
    REQUIRE_FALSE(max_message_bytes_compatible(8u * 1024u * 1024u, 4u * 1024u * 1024u, k_unlimited_message_bytes));
}

TEST_CASE("qos rxo: a pub-max over the sub-requested-max refuses (strict) / degrades (permissive)")
{
    // The offered topic declares a 16 MiB per-message max; the subscriber requests a
    // 4 MiB ceiling. The publisher can emit larger than the subscriber accepts -> the
    // incompatible direction.
    topic_qos offered{};
    offered.max_message_bytes = 16u * 1024u * 1024u;

    const subscriber_qos strict{.durability_mode = durability::none, .requested_max_message_bytes = 4u * 1024u * 1024u, .rxo = rxo_mode::strict};
    const auto r_strict = rxo_check(offered, false, k_test_global_default, k_unlimited_message_bytes, strict);
    REQUIRE(r_strict.verdict == rxo_verdict::incompatible_qos);
    REQUIRE((r_strict.degraded_fields & k_rxo_field_max_message_bytes) != 0);

    const subscriber_qos perm{.durability_mode = durability::none, .requested_max_message_bytes = 4u * 1024u * 1024u, .rxo = rxo_mode::permissive};
    const auto r_perm = rxo_check(offered, false, k_test_global_default, k_unlimited_message_bytes, perm);
    REQUIRE(r_perm.verdict == rxo_verdict::degraded);
    REQUIRE((r_perm.degraded_fields & k_rxo_field_max_message_bytes) != 0);
}

TEST_CASE("qos rxo: a 0=unset offered max resolves through global_default before the size compare")
{
    // The offered topic declares NO per-message max (0=unset), so its effective-max is
    // the node default. A subscriber whose ceiling is BELOW that default is incompatible;
    // one AT/ABOVE it is compatible — proving the resolution runs before the compare.
    topic_qos offered{}; // max_message_bytes == 0
    REQUIRE(offered.max_message_bytes == 0);

    const subscriber_qos too_small{.durability_mode = durability::none, .requested_max_message_bytes = static_cast<std::uint32_t>(k_test_global_default / 2), .rxo = rxo_mode::strict};
    const auto r_small = rxo_check(offered, false, k_test_global_default, k_unlimited_message_bytes, too_small);
    REQUIRE(r_small.verdict == rxo_verdict::incompatible_qos);
    REQUIRE((r_small.degraded_fields & k_rxo_field_max_message_bytes) != 0);

    const subscriber_qos fits{.durability_mode = durability::none, .requested_max_message_bytes = static_cast<std::uint32_t>(k_test_global_default), .rxo = rxo_mode::strict};
    const auto r_fits = rxo_check(offered, false, k_test_global_default, k_unlimited_message_bytes, fits);
    REQUIRE(r_fits.verdict == rxo_verdict::compatible);
    REQUIRE(r_fits.degraded_fields == 0);
}

TEST_CASE("qos rxo: a 0=unset requested max over an unlimited leg is compatible regardless of the offered max")
{
    // The subscriber declares NO ceiling and the leg carries any size -> compatible even
    // against a huge offered per-message max.
    topic_qos offered{};
    offered.max_message_bytes = 64u * 1024u * 1024u;

    const subscriber_qos req{.durability_mode = durability::none, .requested_max_message_bytes = 0, .rxo = rxo_mode::strict};
    const auto r = rxo_check(offered, false, k_test_global_default, k_unlimited_message_bytes, req);
    REQUIRE(r.verdict == rxo_verdict::compatible);
    REQUIRE((r.degraded_fields & k_rxo_field_max_message_bytes) == 0);
}

TEST_CASE("qos rxo: an offer over the leg ceiling is incompatible even with no requested max")
{
    constexpr std::size_t k_leg = 1024;
    REQUIRE(max_message_bytes_compatible(k_leg, 0, k_leg));
    REQUIRE_FALSE(max_message_bytes_compatible(k_leg + 1, 0, k_leg));
    REQUIRE_FALSE(max_message_bytes_compatible(2 * k_leg, 4096u, k_leg));
    REQUIRE_FALSE(max_message_bytes_compatible(k_leg, 512u, 4 * k_leg));

    const topic_qos offered{};
    const auto r_strict = rxo_check(offered, false, k_test_global_default, k_leg, subscriber_qos{.durability_mode = durability::none, .rxo = rxo_mode::strict});
    REQUIRE(r_strict.verdict == rxo_verdict::incompatible_qos);
    REQUIRE(r_strict.degraded_fields == k_rxo_field_max_message_bytes);

    const auto r_perm = rxo_check(offered, false, k_test_global_default, k_leg, subscriber_qos{.durability_mode = durability::none, .rxo = rxo_mode::permissive});
    REQUIRE(r_perm.verdict == rxo_verdict::degraded);
    REQUIRE(r_perm.degraded_fields == k_rxo_field_max_message_bytes);

    const topic_qos narrow{.max_message_bytes = 512u};
    REQUIRE(rxo_check(narrow, false, k_test_global_default, k_leg, subscriber_qos{.durability_mode = durability::none, .rxo = rxo_mode::strict}).verdict == rxo_verdict::compatible);
}

TEST_CASE("qos rxo: an unlimited leg leaves every size verdict as it was")
{
    constexpr std::array<std::size_t, 6> k_offers     = {0, 1, 1024, 4u * 1024u * 1024u, k_test_global_default, k_unlimited_message_bytes};
    constexpr std::array<std::uint32_t, 5> k_requests = {0, 1, 1024, 4u * 1024u * 1024u, 0xFFFFFFFFu};
    for(const std::size_t offer : k_offers)
        for(const std::uint32_t request : k_requests)
            REQUIRE(max_message_bytes_compatible(offer, request, k_unlimited_message_bytes) == (request == 0 || offer <= request));
}
