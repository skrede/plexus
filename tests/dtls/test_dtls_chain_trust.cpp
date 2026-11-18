#include "dtls_relay_link.h"
#include "dtls_chain_authority.h"
#include "test_dtls_handshake_common.h"

#include "plexus/testing/pump.h"

#include <chrono>
#include <utility>

namespace {

bool handshake(pdt::relay_link &l, std::chrono::milliseconds bound)
{
    plexus::testing::pump_until(l.io, [&] { return (l.accepted && l.dialed) || l.dial_failed; }, bound);
    return l.accepted && l.dialed;
}

}

TEST_CASE("dtls.verify: a pinned leaf presented with its intermediate completes without the root in the store", "[dtls][verify][chain]")
{
    pdt::chain_authority ca;
    pdt::identity_fixture srv("chain_trust_srv");
    pdt::identity_fixture cli("chain_trust_cli");
    ca.issue(srv);
    ca.issue(cli);
    auto server_cred = pdt::pin_one(srv, cli.digest);
    auto client_cred = pdt::pin_one(cli, srv.digest);
    ca.present(server_cred);
    ca.present(client_cred);
    pdt::relay_link l(std::move(server_cred), std::move(client_cred));
    REQUIRE(handshake(l, std::chrono::seconds(6)));
}

TEST_CASE("dtls.verify: a pin that misses the leaf refuses a chain the store validates", "[dtls][verify][chain]")
{
    pdt::chain_authority ca;
    pdt::identity_fixture srv("chain_miss_srv");
    pdt::identity_fixture cli("chain_miss_cli");
    pdt::identity_fixture other("chain_miss_oth");
    ca.issue(srv);
    ca.issue(cli);
    auto server_cred = pdt::pin_one(srv, other.digest);
    auto client_cred = pdt::pin_one(cli, other.digest);
    ca.install(server_cred);
    ca.install(client_cred);
    pdt::relay_link l(std::move(server_cred), std::move(client_cred));
    REQUIRE_FALSE(handshake(l, std::chrono::milliseconds(1500)));
    REQUIRE(l.accepted == nullptr);
    REQUIRE(l.dialed == nullptr);
}
