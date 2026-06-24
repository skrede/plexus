#include "test_dtls_mtu_common.h"

using namespace dtls_mtu_fixture;

TEST_CASE("dtls.mtu: a frame at the data-MTU rides one record, one byte over fragments byte-equal, "
          "looped",
          "[dtls][mtu]")
{
    pdt::identity_fixture srv("mtu_srv");
    pdt::identity_fixture cli("mtu_cli");

    constexpr int k_iterations = 100;
    int           proven       = 0;
    std::size_t   observed_cap = 0;

    for(int i = 0; i < k_iterations; ++i)
    {
        // A high configured cap so DTLS_get_data_mtu (the encrypted-record budget) is
        // the binding term of min(configured_cap, DTLS_get_data_mtu) (R-1).
        mtu_link l(srv, cli, /*max_payload=*/100000);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // Find the exact accept/reject boundary. The single-record ceiling is the
        // encrypted record budget DTLS_get_data_mtu reports — the configured record MTU
        // (default_record_mtu) minus the DTLS 1.2 AEAD-GCM record overhead (13B header +
        // 8B explicit IV + 16B auth tag = 37B) minus the 3B udp envelope — so it lands
        // strictly below the configured budget. The bound is DERIVED from the record MTU
        // and a worst-case overhead ceiling, not a hardcoded number, so it tracks the
        // mtu_budget default rather than restating a stale 1400-era constant.
        constexpr std::size_t k_record_mtu          = ptls::dtls_channel::default_record_mtu;
        constexpr std::size_t k_max_record_overhead = 37u + plexus::wire::udp_envelope_overhead; // DTLS 1.2 AEAD-GCM record + udp envelope
        const std::size_t     cap                   = l.probe_one_record_ceiling(100, k_record_mtu);
        REQUIRE(cap >= k_record_mtu - k_max_record_overhead); // within the overhead band below the record MTU
        REQUIRE(cap < k_record_mtu);                          // strictly below (overhead subtracted)
        observed_cap = cap;

        // The boundary frame is delivered intact AS ONE record (no fragmentation): the
        // server got exactly one on_data of exactly `cap` bytes over exactly one record.
        REQUIRE(l.delivered_in_one_record(cap));

        // One byte over the single-record ceiling no longer rejects — it FRAGMENTS across
        // more than one record and reassembles byte-equal into ONE on_data (the new
        // large-payload capability; the encrypted budget still governs each record).
        REQUIRE(l.send_and_deliver(cap + 1));
        REQUIRE(l.client_records > 1);
        REQUIRE_FALSE(l.client_too_large);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
    REQUIRE(observed_cap > 0);
}

TEST_CASE("dtls.mtu: a low configured cap binds the single-record ceiling below the data-MTU, looped", "[dtls][mtu]")
{
    pdt::identity_fixture srv("cap_srv");
    pdt::identity_fixture cli("cap_cli");

    // A configured cap well BELOW DTLS_get_data_mtu so the configured term binds
    // min(configured_cap, DTLS_get_data_mtu) — proving the reject is the MIN of the two,
    // not DTLS_get_data_mtu alone.
    constexpr std::size_t k_cap = 200;

    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        mtu_link l(srv, cli, k_cap);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // At the configured cap minus the envelope: delivered as one record (the configured
        // cap is the binding term, well under the encrypted data MTU).
        REQUIRE(l.delivered_in_one_record(k_cap - plexus::wire::udp_envelope_overhead));
        // Over the configured single-record ceiling: fragments (not rejects) and reassembles
        // byte-equal — the configured cap binds each fragment record, the message still flows.
        REQUIRE(l.send_and_deliver(k_cap + 1));
        REQUIRE(l.client_records > 1);
        REQUIRE_FALSE(l.client_too_large);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
