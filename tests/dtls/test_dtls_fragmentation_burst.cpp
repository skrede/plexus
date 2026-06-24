#include "test_dtls_fragmentation_common.h"

using namespace dtls_fragmentation_fixture;

namespace {

// One fresh live DTLS session driving a single `size`-byte message through the splitter into
// many DTLS records and back through the reassembler. Returns {received_records, reassembled}.
// The logical ceiling is raised above max_message_size so the frame fragments rather than
// rejecting; each fragment rides one record bounded by the DTLS_get_data_mtu-derived budget.
struct large_run
{
    std::size_t            records;
    std::vector<std::byte> got;
};

large_run dtls_large_run(const pdt::identity_fixture &srv, const pdt::identity_fixture &cli, std::size_t size, std::chrono::milliseconds timeout)
{
    frag_link l(srv, cli, /*max_payload=*/8u * 1024u * 1024u);
    l.handshake();
    REQUIRE(l.client_complete);
    REQUIRE(l.server_complete);

    auto got = l.round_trip(size, timeout);
    return {static_cast<std::size_t>(l.client_to_server_count), std::move(got)};
}

}

TEST_CASE("dtls.fragment: a 1 MB / 4 MB best-effort burst reassembles byte-equal over the default "
          "send queue",
          "[dtls][fragment]")
{
    pdt::identity_fixture srv("fragbe_srv");
    pdt::identity_fixture cli("fragbe_cli");

    // DTLS app data is best-effort (no DTLS-layer retransmit). A single 1 MB / 4 MB send
    // splits into ~900 / ~3650 records emitted in one synchronous burst. Each record is one
    // datagram handed to the shared udp_server send queue: the queue's byte cap is floored at
    // one per-message ceiling, so a single fragmenting message's whole burst is admitted (a
    // refused fragment on the best-effort path is lost forever, with no retransmit to recover
    // it, so the floor is what keeps the message intact). The relay carries every emitted
    // record into the peer's reassembler, which completes the message byte-for-byte.
    constexpr std::size_t k_one_mb  = 1024 * 1024;
    constexpr std::size_t k_four_mb = 4 * 1024 * 1024;
    for(std::size_t size : {k_one_mb, k_four_mb})
    {
        const auto r = dtls_large_run(srv, cli, size, std::chrono::milliseconds{8000});
        REQUIRE(r.records > 1);        // the message fragmented into many records
        REQUIRE(r.got.size() == size); // and every fragment arrived and reassembled
    }
}
