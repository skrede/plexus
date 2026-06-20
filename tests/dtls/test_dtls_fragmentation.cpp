#include "test_dtls_fragmentation_common.h"

using namespace dtls_fragmentation_fixture;

TEST_CASE("dtls.fragment: a large payload fragments across records and reassembles byte-equal over "
          "a live session, looped",
          "[dtls][fragment]")
{
    pdt::identity_fixture srv("frag_srv");
    pdt::identity_fixture cli("frag_cli");

    // Largest payload proven over live DTLS: ~24 KiB. With a high logical ceiling (so the
    // frame fragments rather than rejecting) and the default record MTU (~1363 encrypted),
    // a 24 KiB message splits into ~18 records and reassembles into ONE on_data byte-equal.
    constexpr std::size_t k_payload = 24u * 1024u;

    constexpr int k_iterations  = 50;
    int           proven        = 0;
    int           max_fragments = 0;

    for(int i = 0; i < k_iterations; ++i)
    {
        frag_link l(srv, cli, /*max_payload=*/1u * 1024u * 1024u);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        const auto got = l.round_trip(k_payload);
        REQUIRE(got.size() == k_payload);
        REQUIRE(got == ramp(k_payload)); // byte-equal reassembly
        // The message crossed the wire as MANY records (fragmentation happened), each one
        // DTLS record — never one oversize record, never a reject.
        REQUIRE(l.client_to_server_count > 1);
        max_fragments = std::max(max_fragments, l.client_to_server_count);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
    REQUIRE(max_fragments > 1);
}

TEST_CASE("dtls.fragment: a frame at the record budget rides ONE record (parity, no "
          "fragmentation), looped",
          "[dtls][fragment]")
{
    pdt::identity_fixture srv("par_srv");
    pdt::identity_fixture cli("par_cli");

    constexpr int k_iterations = 50;
    int           proven       = 0;

    for(int i = 0; i < k_iterations; ++i)
    {
        // A high logical ceiling so DTLS_get_data_mtu (the encrypted record budget) is the
        // binding term. A frame comfortably under that budget must ride ONE record
        // (byte-identical to the pre-fragment path), not fragment.
        frag_link l(srv, cli, /*max_payload=*/100000);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // The encrypted record budget is the configured record MTU (default_record_mtu)
        // minus the DTLS 1.2 AEAD-GCM record overhead (~37B) minus the 3B udp envelope. A
        // frame an overhead-band below the record MTU stays comfortably under the budget and
        // crosses as exactly one record. The size is DERIVED from the record MTU, not a stale
        // 1400-era constant, so it tracks the mtu_budget default.
        constexpr std::size_t k_under_budget = ptls::dtls_channel::default_record_mtu - 128u;
        const auto            got            = l.round_trip(k_under_budget);
        REQUIRE(got.size() == k_under_budget);
        REQUIRE(got == ramp(k_under_budget));
        REQUIRE(l.client_to_server_count == 1); // ONE record: no fragmentation
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
