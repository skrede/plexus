#include "test_qos_rxo_common.h"

using namespace qos_rxo_fixture;

TEST_CASE("qos rxo: max-message-bytes relation")
{
    // The publisher's effective-max must fit the subscriber's requested ceiling
    // (offered <= requested). 0 requested is "unset" and always compatible; the
    // offered side is already the resolved effective-max at the call site.
    REQUIRE(max_message_bytes_compatible(4u * 1024u * 1024u, 0)); // unset request
    REQUIRE(max_message_bytes_compatible(1u * 1024u * 1024u, 4u * 1024u * 1024u));
    REQUIRE(max_message_bytes_compatible(4u * 1024u * 1024u, 4u * 1024u * 1024u));
    REQUIRE_FALSE(max_message_bytes_compatible(8u * 1024u * 1024u, 4u * 1024u * 1024u));
}

TEST_CASE("qos rxo: a pub-max over the sub-requested-max refuses (strict) / degrades (permissive)")
{
    // The offered topic declares a 16 MiB per-message max; the subscriber requests a
    // 4 MiB ceiling. The publisher can emit larger than the subscriber accepts -> the
    // incompatible direction.
    topic_qos offered{};
    offered.max_message_bytes = 16u * 1024u * 1024u;

    const subscriber_qos strict{.durability_mode             = durability::none,
                                .requested_max_message_bytes = 4u * 1024u * 1024u,
                                .rxo                         = rxo_mode::strict};
    const auto           r_strict = rxo_check(offered, false, k_test_global_default, strict);
    REQUIRE(r_strict.verdict == rxo_verdict::incompatible_qos);
    REQUIRE((r_strict.degraded_fields & k_rxo_field_max_message_bytes) != 0);

    const subscriber_qos perm{.durability_mode             = durability::none,
                              .requested_max_message_bytes = 4u * 1024u * 1024u,
                              .rxo                         = rxo_mode::permissive};
    const auto           r_perm = rxo_check(offered, false, k_test_global_default, perm);
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

    const subscriber_qos too_small{.durability_mode = durability::none,
                                   .requested_max_message_bytes =
                                           static_cast<std::uint32_t>(k_test_global_default / 2),
                                   .rxo = rxo_mode::strict};
    const auto           r_small = rxo_check(offered, false, k_test_global_default, too_small);
    REQUIRE(r_small.verdict == rxo_verdict::incompatible_qos);
    REQUIRE((r_small.degraded_fields & k_rxo_field_max_message_bytes) != 0);

    const subscriber_qos fits{.durability_mode = durability::none,
                              .requested_max_message_bytes =
                                      static_cast<std::uint32_t>(k_test_global_default),
                              .rxo = rxo_mode::strict};
    const auto           r_fits = rxo_check(offered, false, k_test_global_default, fits);
    REQUIRE(r_fits.verdict == rxo_verdict::compatible);
    REQUIRE(r_fits.degraded_fields == 0);
}

TEST_CASE("qos rxo: a 0=unset requested max is always compatible regardless of the offered max")
{
    // The subscriber declares NO ceiling -> always compatible even against a huge
    // offered per-message max.
    topic_qos offered{};
    offered.max_message_bytes = 64u * 1024u * 1024u;

    const subscriber_qos req{.durability_mode             = durability::none,
                             .requested_max_message_bytes = 0,
                             .rxo                         = rxo_mode::strict};
    const auto           r = rxo_check(offered, false, k_test_global_default, req);
    REQUIRE(r.verdict == rxo_verdict::compatible);
    REQUIRE((r.degraded_fields & k_rxo_field_max_message_bytes) == 0);
}
