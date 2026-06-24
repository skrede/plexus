#include "test_dtls_mux_common.h"

using namespace dtls_mux_fixture;

TEST_CASE("dtls.mux_select: the selector classifies dtls as remote, never local (locality exclusion)", "[dtls][mux][select]")
{
    pio::transport_selector sel;
    const auto              reserved = pio::reliability_hint::unspecified;

    // "dtls" is a REMOTE-tier scheme — so locality confinement EXCLUDES it (a
    // host-confined process|local topic never rides dtls even though dtls encrypts).
    // "unix"/"inproc" are the same-host local tier; "tls"/"tcp"/"udp" are remote too.
    REQUIRE(sel.select({"dtls", "127.0.0.1:5000"}, reserved) == pio::transport_kind::remote);
    REQUIRE(sel.select({"unix", "/tmp/s"}, reserved) == pio::transport_kind::local);
    REQUIRE(sel.select({"inproc", "node-a"}, reserved) == pio::transport_kind::local);
    // dtls is secure-best_effort (the secure parallel of udp) on the reliability axis.
    REQUIRE(sel.reliability_of_scheme("dtls") == pio::reliability_hint::best_effort);
}

TEST_CASE("dtls.mux: a dtls dial routes to the secure-datagram member, completes, and flows a "
          "frame, looped",
          "[dtls][mux][route]")
{
    pdt::identity_fixture server_id("mux_srv");
    pdt::identity_fixture client_id("mux_cli");

    const std::string      text = "secure-datagram-over-the-mux";
    std::vector<std::byte> frame(reinterpret_cast<const std::byte *>(text.data()), reinterpret_cast<const std::byte *>(text.data()) + text.size());

    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        mux_pair n(server_id, client_id);
        n.listen_face.mux.listen({"dtls", "127.0.0.1:0"});
        n.dial_face.mux.dial({"dtls", "127.0.0.1:" + std::to_string(n.listen_face.secure_datagram.port())});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr);   // delivered POST external_complete via the dtls member
        REQUIRE(n.accepted != nullptr); // accepted POST mutual-verify via the dtls member
        // The "dtls" scheme survives the type-erasure on BOTH ends.
        REQUIRE(n.dialed->remote_endpoint().scheme == "dtls");
        REQUIRE(n.accepted->remote_endpoint().scheme == "dtls");
        // The dialed endpoint is returned UNCHANGED (the correlation key).
        REQUIRE(n.dialed_ep.has_value());
        REQUIRE(n.dialed_ep->scheme == "dtls");

        // An app frame flows decrypted over the wrapped polymorphic_byte_channel — proves the route
        // landed a live secure-datagram channel, not merely a completed handshake.
        std::vector<std::byte> got;
        bool                   received = false;
        n.accepted->on_data(
                [&](std::span<const std::byte> d)
                {
                    got.assign(d.begin(), d.end());
                    received = true;
                });
        n.dialed->send(std::span<const std::byte>{frame});
        n.pump_until([&] { return received; });
        REQUIRE(received);
        REQUIRE(got == frame);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}
