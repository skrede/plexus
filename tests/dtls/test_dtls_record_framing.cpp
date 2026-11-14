#include "dtls_relay_link.h"
#include "dtls_chain_authority.h"
#include "test_dtls_handshake_common.h"

#include "plexus/testing/pump.h"

#include <span>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace {

constexpr std::size_t k_record_header = 13;

bool record_header(std::span<const std::byte> at)
{
    const auto type = std::to_integer<std::uint8_t>(at[0]);
    return at.size() >= k_record_header && type >= 20 && type <= 26 && std::to_integer<std::uint8_t>(at[1]) == 0xfe;
}

// RFC 6347 section 4.1: a record is a 13-byte header whose big-endian length at bytes 11..12 counts the fragment after
// it. A datagram that starts or ends mid-record counts zero records.
std::size_t whole_records(std::span<const std::byte> datagram)
{
    std::size_t offset  = 0;
    std::size_t records = 0;
    while(offset < datagram.size())
    {
        const auto at = datagram.subspan(offset);
        if(!record_header(at))
            return 0;
        offset += k_record_header + (std::to_integer<std::size_t>(at[11]) << 8 | std::to_integer<std::size_t>(at[12]));
        ++records;
    }
    return offset == datagram.size() ? records : 0;
}

bool handshake(pdt::relay_link &l)
{
    plexus::testing::pump_until(l.io, [&] { return (l.accepted && l.dialed) || l.dial_failed; });
    return l.accepted && l.dialed;
}

}

TEST_CASE("dtls.framing: RSA-3072 identities complete a handshake", "[dtls][framing]")
{
    pdt::identity_fixture srv("rsa_srv", 3072);
    pdt::identity_fixture cli("rsa_cli", 3072);
    pdt::relay_link l(pdt::pin_one(srv, cli.digest), pdt::pin_one(cli, srv.digest));
    REQUIRE(handshake(l));
}

TEST_CASE("dtls.framing: a leaf + intermediate RSA-2048 chain completes a handshake", "[dtls][framing]")
{
    pdt::chain_authority ca;
    pdt::identity_fixture srv("chain_srv");
    pdt::identity_fixture cli("chain_cli");
    ca.issue(srv);
    ca.issue(cli);
    auto server_cred = pdt::pin_one(srv, cli.digest);
    auto client_cred = pdt::pin_one(cli, srv.digest);
    ca.install(server_cred);
    ca.install(client_cred);
    pdt::relay_link l(std::move(server_cred), std::move(client_cred));
    REQUIRE(handshake(l));
}

TEST_CASE("dtls.framing: a handshake flight over one datagram is sent as whole records, data one record each", "[dtls][framing]")
{
    pdt::identity_fixture srv("pack_srv", 3072);
    pdt::identity_fixture cli("pack_cli", 3072);
    pdt::relay_link l(pdt::pin_one(srv, cli.digest), pdt::pin_one(cli, srv.digest));
    const bool completed = handshake(l);

    std::size_t cut   = 0;
    std::size_t multi = 0;
    for(const auto &datagram : l.link->datagrams)
    {
        const std::size_t records = whole_records(datagram);
        cut += records == 0 ? 1 : 0;
        multi += records > 1 ? 1 : 0;
    }
    CHECK(cut == 0);
    CHECK(multi > 0);
    REQUIRE(completed);

    std::size_t received = 0;
    l.accepted->on_data([&](std::span<const std::byte>) { ++received; });
    l.link->datagrams.clear();
    const std::vector<std::byte> payload(1000);
    for(int i = 0; i < 20; ++i)
        l.dialed->send(payload);
    REQUIRE(plexus::testing::pump_until(l.io, [&] { return received == 20; }));

    std::size_t data_datagrams = 0;
    for(const auto &datagram : l.link->datagrams)
    {
        if(std::to_integer<std::uint8_t>(datagram[0]) != 23)
            continue;
        ++data_datagrams;
        CHECK(whole_records(datagram) == 1);
    }
    CHECK(data_datagrams == 20);
}
