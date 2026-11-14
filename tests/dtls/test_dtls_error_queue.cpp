#include "dtls_relay_link.h"
#include "test_dtls_handshake_common.h"

#include "plexus/testing/pump.h"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <span>
#include <vector>
#include <cstddef>

// OpenSSL's error queue is per thread, so an entry another channel's failed dial left behind is still on the io thread
// when a healthy channel next asks SSL_get_error why a read returned no data.
TEST_CASE("dtls.error_queue: a stale OpenSSL error on the io thread does not fail a healthy channel's read", "[dtls][error_queue]")
{
    pdt::identity_fixture srv("errq_srv");
    pdt::identity_fixture cli("errq_cli");
    pdt::relay_link l(pdt::pin_one(srv, cli.digest), pdt::pin_one(cli, srv.digest));
    REQUIRE(plexus::testing::pump_until(l.io, [&] { return (l.accepted && l.dialed) || l.dial_failed; }));
    REQUIRE((l.accepted && l.dialed));

    std::vector<std::byte> received;
    bool errored = false;
    l.accepted->on_data([&](std::span<const std::byte> frame) { received.assign(frame.begin(), frame.end()); });
    l.accepted->on_error([&](plexus::io::io_error) { errored = true; });
    l.link->script.push_back(pdt::action::hold);
    const std::vector<std::byte> payload(64, std::byte{0x5a});
    l.dialed->send(payload);
    REQUIRE(plexus::testing::pump_until(l.io, [&] { return l.link->held.size() == 1; }));

    ERR_raise(ERR_LIB_SSL, SSL_R_READ_TIMEOUT_EXPIRED);
    l.accepted->deliver_inbound(l.link->held.front());
    REQUIRE(plexus::testing::pump_until(l.io, [&] { return !received.empty() || errored; }));
    CHECK_FALSE(errored);
    CHECK(l.accepted->is_open());
    CHECK(received == payload);
}
