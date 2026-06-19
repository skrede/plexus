// The pure request-vs-offered relation oracle: every comparable field's compatibility
// matrix (compatible AND incompatible rows per the offered>=requested direction), the
// always-hard source-identity field refusing regardless of the subscriber's chosen
// mode, strict mode yielding incompatible_qos with the failing-field reason bitmask,
// and permissive mode yielding a degraded verdict with a NON-EMPTY bitmask naming the
// right soft field (the non-silent guarantee at the pure level). Driven directly
// against the relations — no I/O, no registry.

#include "plexus/io/qos_rxo.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/reliability.h"
#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>

using plexus::topic_qos;
using plexus::io::durability;
using plexus::io::reliability;
using plexus::io::rxo_mode;
using plexus::io::rxo_verdict;
using plexus::io::subscriber_qos;
using plexus::io::rxo_check;
using plexus::io::reliability_compatible;
using plexus::io::durability_compatible;
using plexus::io::deadline_compatible;
using plexus::io::lease_compatible;
using plexus::io::source_identity_compatible;
using plexus::io::k_rxo_field_reliability;
using plexus::io::k_rxo_field_durability;
using plexus::io::k_rxo_field_deadline;
using plexus::io::k_rxo_field_lease;
using plexus::io::k_rxo_field_max_message_bytes;
using plexus::io::max_message_bytes_compatible;

namespace {
// The node-level per-message default the size relation resolves an offered topic's
// 0=unset max against. Sized away from the round numbers the cases use so the
// effective-max resolution is observable.
constexpr std::size_t k_test_global_default = 8u * 1024u * 1024u;
}

TEST_CASE("qos rxo: reliability relation")
{
    // A reliable request needs a reliable offer; a best-effort request matches any.
    REQUIRE_FALSE(reliability_compatible(reliability::best_effort, true));
    REQUIRE(reliability_compatible(reliability::reliable, true));
    REQUIRE(reliability_compatible(reliability::reliable, false));
    REQUIRE(reliability_compatible(reliability::best_effort, false));
}

TEST_CASE("qos rxo: durability relation")
{
    // A non-latching offer (none) cannot satisfy a latest/all request; any offer
    // satisfies a none request; a latching offer satisfies any request.
    REQUIRE_FALSE(durability_compatible(false, durability::latest));
    REQUIRE_FALSE(durability_compatible(false, durability::all));
    REQUIRE(durability_compatible(false, durability::none));
    REQUIRE(durability_compatible(true, durability::none));
    REQUIRE(durability_compatible(true, durability::latest));
    REQUIRE(durability_compatible(true, durability::all));
}

TEST_CASE("qos rxo: deadline and lease relations")
{
    // 0 on either side is "unset" and always compatible; otherwise offered<=requested.
    REQUIRE(deadline_compatible(0, 100));
    REQUIRE(deadline_compatible(100, 0));
    REQUIRE(deadline_compatible(50, 100));
    REQUIRE(deadline_compatible(100, 100));
    REQUIRE_FALSE(deadline_compatible(200, 100));

    REQUIRE(lease_compatible(0, 100));
    REQUIRE(lease_compatible(100, 0));
    REQUIRE(lease_compatible(50, 100));
    REQUIRE(lease_compatible(100, 100));
    REQUIRE_FALSE(lease_compatible(200, 100));
}

TEST_CASE("qos rxo: source identity refuses regardless of mode")
{
    // The relation itself: requires + not-offered is the only incompatible row.
    REQUIRE_FALSE(source_identity_compatible(false, true));
    REQUIRE(source_identity_compatible(true, true));
    REQUIRE(source_identity_compatible(false, false));
    REQUIRE(source_identity_compatible(true, false));

    const topic_qos offered{}; // offers no source identity
    for(auto mode : {rxo_mode::permissive, rxo_mode::strict})
    {
        const subscriber_qos req{.requires_source_identity = true, .rxo = mode};
        const auto           r =
                rxo_check(offered, /*offers_source_identity=*/false, k_test_global_default, req);
        REQUIRE(r.verdict == rxo_verdict::source_identity_incompatible);
        REQUIRE(r.degraded_fields == 0); // not a degradable field
    }

    // Offered => admitted; not required => admitted regardless of the offer. The
    // requests pin durability::none so the non-latching offer is clean on that axis
    // (the friendly latest default is intentionally degraded against a non-latch topic).
    REQUIRE(rxo_check(offered, /*offers=*/true, k_test_global_default,
                      subscriber_qos{.durability_mode          = durability::none,
                                     .requires_source_identity = true})
                    .verdict == rxo_verdict::compatible);
    REQUIRE(rxo_check(offered, /*offers=*/false, k_test_global_default,
                      subscriber_qos{.durability_mode          = durability::none,
                                     .requires_source_identity = false})
                    .verdict == rxo_verdict::compatible);
}

TEST_CASE("qos rxo: strict mode refuses a soft mismatch with a reason")
{
    const topic_qos offered{}; // best_effort, !latch — offers the weak classes

    // A single soft mismatch: a reliable request (durability::none so only reliability
    // mismatches) under strict refuses with ONLY the reliability bit set (the reason
    // is non-vacuous and exact).
    const subscriber_qos one{.durability_mode                = durability::none,
                             .requested_reliability_reliable = true,
                             .rxo                            = rxo_mode::strict};
    const auto           r1 = rxo_check(offered, false, k_test_global_default, one);
    REQUIRE(r1.verdict == rxo_verdict::incompatible_qos);
    REQUIRE(r1.degraded_fields == k_rxo_field_reliability);

    // A multi-field mismatch sets multiple bits.
    const subscriber_qos many{.durability_mode                = durability::all,
                              .requested_reliability_reliable = true,
                              .rxo                            = rxo_mode::strict};
    const auto           r2 = rxo_check(offered, false, k_test_global_default, many);
    REQUIRE(r2.verdict == rxo_verdict::incompatible_qos);
    REQUIRE((r2.degraded_fields & k_rxo_field_reliability) != 0);
    REQUIRE((r2.degraded_fields & k_rxo_field_durability) != 0);
}

TEST_CASE("qos rxo: permissive mode surfaces the degraded field set")
{
    const topic_qos offered{}; // best_effort, !latch

    // The SAME soft mismatch under permissive connects with a NON-EMPTY surfaced set
    // that names the right field — the non-silent guarantee at the pure level.
    const subscriber_qos req{.durability_mode                = durability::none,
                             .requested_reliability_reliable = true,
                             .rxo                            = rxo_mode::permissive};
    const auto           r = rxo_check(offered, false, k_test_global_default, req);
    REQUIRE(r.verdict == rxo_verdict::degraded);
    REQUIRE(r.degraded_fields != 0);
    REQUIRE((r.degraded_fields & k_rxo_field_reliability) != 0);

    // A deadline soft mismatch surfaces the deadline bit.
    topic_qos slow{};
    slow.offered_deadline_ns = 200;
    const subscriber_qos req_dl{.durability_mode       = durability::none,
                                .requested_deadline_ns = 100,
                                .rxo                   = rxo_mode::permissive};
    const auto           r_dl = rxo_check(slow, false, k_test_global_default, req_dl);
    REQUIRE(r_dl.verdict == rxo_verdict::degraded);
    REQUIRE((r_dl.degraded_fields & k_rxo_field_deadline) != 0);

    // A compatible pair under permissive is clean — no degradation. (durability::none
    // so the non-latching offer is clean on that axis.)
    const auto ok = rxo_check(
            offered, false, k_test_global_default,
            subscriber_qos{.durability_mode = durability::none, .rxo = rxo_mode::permissive});
    REQUIRE(ok.verdict == rxo_verdict::compatible);
    REQUIRE(ok.degraded_fields == 0);
}

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
