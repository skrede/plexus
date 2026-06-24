#include "test_qos_rxo_common.h"

using namespace qos_rxo_fixture;

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
        const auto           r = rxo_check(offered, /*offers_source_identity=*/false, k_test_global_default, req);
        REQUIRE(r.verdict == rxo_verdict::source_identity_incompatible);
        REQUIRE(r.degraded_fields == 0); // not a degradable field
    }

    // Offered => admitted; not required => admitted regardless of the offer. The
    // requests pin durability::none so the non-latching offer is clean on that axis
    // (the friendly latest default is intentionally degraded against a non-latch topic).
    REQUIRE(rxo_check(offered, /*offers=*/true, k_test_global_default, subscriber_qos{.durability_mode = durability::none, .requires_source_identity = true}).verdict ==
            rxo_verdict::compatible);
    REQUIRE(rxo_check(offered, /*offers=*/false, k_test_global_default, subscriber_qos{.durability_mode = durability::none, .requires_source_identity = false}).verdict ==
            rxo_verdict::compatible);
}

TEST_CASE("qos rxo: strict mode refuses a soft mismatch with a reason")
{
    const topic_qos offered{}; // best_effort, !latch — offers the weak classes

    // A single soft mismatch: a reliable request (durability::none so only reliability
    // mismatches) under strict refuses with ONLY the reliability bit set (the reason
    // is non-vacuous and exact).
    const subscriber_qos one{.durability_mode = durability::none, .requested_reliability_reliable = true, .rxo = rxo_mode::strict};
    const auto           r1 = rxo_check(offered, false, k_test_global_default, one);
    REQUIRE(r1.verdict == rxo_verdict::incompatible_qos);
    REQUIRE(r1.degraded_fields == k_rxo_field_reliability);

    // A multi-field mismatch sets multiple bits.
    const subscriber_qos many{.durability_mode = durability::all, .requested_reliability_reliable = true, .rxo = rxo_mode::strict};
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
    const subscriber_qos req{.durability_mode = durability::none, .requested_reliability_reliable = true, .rxo = rxo_mode::permissive};
    const auto           r = rxo_check(offered, false, k_test_global_default, req);
    REQUIRE(r.verdict == rxo_verdict::degraded);
    REQUIRE(r.degraded_fields != 0);
    REQUIRE((r.degraded_fields & k_rxo_field_reliability) != 0);

    // A deadline soft mismatch surfaces the deadline bit.
    topic_qos slow{};
    slow.offered_deadline_ns = 200;
    const subscriber_qos req_dl{.durability_mode = durability::none, .requested_deadline_ns = 100, .rxo = rxo_mode::permissive};
    const auto           r_dl = rxo_check(slow, false, k_test_global_default, req_dl);
    REQUIRE(r_dl.verdict == rxo_verdict::degraded);
    REQUIRE((r_dl.degraded_fields & k_rxo_field_deadline) != 0);

    // A compatible pair under permissive is clean — no degradation. (durability::none
    // so the non-latching offer is clean on that axis.)
    const auto ok = rxo_check(offered, false, k_test_global_default, subscriber_qos{.durability_mode = durability::none, .rxo = rxo_mode::permissive});
    REQUIRE(ok.verdict == rxo_verdict::compatible);
    REQUIRE(ok.degraded_fields == 0);
}
