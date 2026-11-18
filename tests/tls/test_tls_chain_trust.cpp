#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_transport.h"
#include "plexus/tls/tls_credential.h"

#include "plexus/io/security/verify_policy.h"

#include "dtls/dtls_chain_authority.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <utility>

namespace pdt  = plexus::dtls_test;
namespace ptls = plexus::tls;
namespace pio  = plexus::io;

namespace {

// Accepts every leaf, leaving the trust decision to the engine's chain validation.
class chain_policy final : public pio::security::verify_policy
{
public:
    bool decide(const pio::security::cert_facts &) const noexcept override
    {
        return true;
    }
};

ptls::tls_credential credential(const pdt::identity_fixture &self, std::shared_ptr<const pio::security::verify_policy> policy)
{
    return ptls::load_credential(self.cert_path.string(), self.key_path.string(), std::move(policy));
}

ptls::tls_credential pin(const pdt::identity_fixture &self, const pdt::spki_digest &peer)
{
    return credential(self, std::make_shared<const pio::security::spki_pin_policy>(std::vector<pdt::spki_digest>{peer}));
}

struct chain_link
{
    ::asio::io_context io;
    ptls::tls_credential accepter_cred;
    ptls::tls_credential dialer_cred;
    ptls::tls_transport accepter;
    ptls::tls_transport dialer;
    std::unique_ptr<ptls::tls_channel> accepted;
    std::unique_ptr<ptls::tls_channel> dialed;

    chain_link(ptls::tls_credential a, ptls::tls_credential d)
            : accepter_cred(std::move(a))
            , dialer_cred(std::move(d))
            , accepter(io, accepter_cred)
            , dialer(io, dialer_cred)
    {
        accepter.on_accepted([this](std::unique_ptr<ptls::tls_channel> ch) { accepted = std::move(ch); });
        dialer.on_dialed([this](std::unique_ptr<ptls::tls_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        accepter.listen({"tls", "127.0.0.1:0"});
        dialer.dial({"tls", "127.0.0.1:" + std::to_string(accepter.port())});
    }

    bool handshake(std::chrono::milliseconds bound)
    {
        plexus::testing::pump_until(io, [this] { return accepted && dialed; }, bound);
        return accepted && dialed;
    }
};

struct chained_pair
{
    pdt::chain_authority ca;
    pdt::identity_fixture acc;
    pdt::identity_fixture dia;

    chained_pair()
            : ca()
            , acc("tls_chain_acc")
            , dia("tls_chain_dia")
    {
        ca.issue(acc);
        ca.issue(dia);
    }
};

}

TEST_CASE("tls trust: a pinned leaf presented with its intermediate completes without the root in the store", "[tls][trust][chain]")
{
    chained_pair p;
    auto accepter_cred = pin(p.acc, p.dia.digest);
    auto dialer_cred   = pin(p.dia, p.acc.digest);
    p.ca.present(accepter_cred);
    p.ca.present(dialer_cred);
    chain_link l(std::move(accepter_cred), std::move(dialer_cred));
    REQUIRE(l.handshake(std::chrono::seconds(5)));
}

TEST_CASE("tls trust: a pin that misses the leaf refuses a chain the store validates", "[tls][trust][chain][failclosed]")
{
    chained_pair p;
    pdt::identity_fixture other("tls_chain_oth");
    auto accepter_cred = pin(p.acc, other.digest);
    auto dialer_cred   = pin(p.dia, other.digest);
    p.ca.install(accepter_cred);
    p.ca.install(dialer_cred);
    chain_link l(std::move(accepter_cred), std::move(dialer_cred));
    REQUIRE_FALSE(l.handshake(std::chrono::milliseconds(1500)));
    REQUIRE(l.accepted == nullptr);
    REQUIRE(l.dialed == nullptr);
}

TEST_CASE("tls trust: a chain-anchored policy refuses a chain whose root the store lacks", "[tls][trust][chain][failclosed]")
{
    chained_pair p;
    auto accepter_cred = credential(p.acc, std::make_shared<const chain_policy>());
    auto dialer_cred   = credential(p.dia, std::make_shared<const chain_policy>());
    p.ca.present(accepter_cred);
    p.ca.present(dialer_cred);
    chain_link l(std::move(accepter_cred), std::move(dialer_cred));
    REQUIRE_FALSE(l.handshake(std::chrono::milliseconds(1500)));
    REQUIRE(l.accepted == nullptr);
    REQUIRE(l.dialed == nullptr);
}

TEST_CASE("tls trust: a chain-anchored policy completes once the store holds the root", "[tls][trust][chain]")
{
    chained_pair p;
    auto accepter_cred = credential(p.acc, std::make_shared<const chain_policy>());
    auto dialer_cred   = credential(p.dia, std::make_shared<const chain_policy>());
    p.ca.install(accepter_cred);
    p.ca.install(dialer_cred);
    chain_link l(std::move(accepter_cred), std::move(dialer_cred));
    REQUIRE(l.handshake(std::chrono::seconds(5)));
}
