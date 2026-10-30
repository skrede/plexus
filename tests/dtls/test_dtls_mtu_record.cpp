#include "test_dtls_mtu_common.h"

using namespace dtls_mtu_fixture;

TEST_CASE("dtls.mtu: a record MTU above 2048 bytes rides one record intact, looped", "[dtls][mtu]")
{
    pdt::identity_fixture srv("big_mtu_srv");
    pdt::identity_fixture cli("big_mtu_cli");

    // The drain buffer is sized from the configured record_mtu: a smaller buffer makes SSL_read
    // (message-oriented) DISCARD a larger record and BIO_read SPLIT one record across datagrams.
    // A record budget well above 2048 bytes and below the 8192 ceiling must carry a frame near
    // that budget as exactly ONE record, delivered byte-equal.
    constexpr std::size_t k_record_mtu = 4096;
    constexpr int k_iterations         = 50;
    int proven                         = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        mtu_link l(srv, cli, /*max_payload=*/100000, k_record_mtu);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // The single-record ceiling tracks the record MTU, within the AEAD-GCM + envelope
        // overhead band below it.
        constexpr std::size_t k_max_record_overhead = 37u + plexus::wire::udp_envelope_overhead;
        const std::size_t cap                       = l.probe_one_record_ceiling(2200, k_record_mtu);
        REQUIRE(cap > 2048);
        REQUIRE(cap >= k_record_mtu - k_max_record_overhead); // within the overhead band below the record MTU
        REQUIRE(cap < k_record_mtu);

        // The ceiling frame (> 2048 B) delivers intact as ONE record, neither truncated by
        // SSL_read nor split by BIO_read.
        REQUIRE(l.delivered_in_one_record(cap));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("dtls.mtu: a frame beyond the bounded max-message size is rejected via "
          "message_too_large, looped",
          "[dtls][mtu]")
{
    pdt::identity_fixture srv("big_srv");
    pdt::identity_fixture cli("big_cli");

    // The oversize reject holds for a genuinely-too-big message: a frame beyond the
    // channel's per-MESSAGE size ceiling cannot fragment (it would exceed the receiver's hard
    // ceiling), so it is rejected at publish via on_error(message_too_large) and nothing
    // crosses the wire (the fail-closed bound on the fragment path). The bound is the
    // configurable node default (global_default_max_message_bytes) the channel is minted with;
    // a frame one byte past it is refused.
    constexpr int k_iterations = 30;
    int proven                 = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        mtu_link l(srv, cli, /*max_payload=*/100000);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        REQUIRE_FALSE(l.send_and_deliver(pio::global_default_max_message_bytes + 1));
        REQUIRE(l.client_too_large);
        REQUIRE(l.server_received.empty());
        REQUIRE(l.client_records == 0); // nothing crossed the wire
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
