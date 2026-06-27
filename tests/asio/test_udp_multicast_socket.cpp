#include "plexus/asio/udp_multicast_socket.h"

#include "plexus/stream/datagram_socket.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/address_v4.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <optional>

namespace {

namespace pasio = plexus::asio;

static_assert(plexus::stream::datagram_socket<pasio::udp_multicast_socket>);

// A TEST-LOCAL group deliberately distinct from the plexus default (239.255.0.7) so a co-resident
// plexus node never collides with the fixture's traffic.
const ::asio::ip::address_v4 k_test_group = ::asio::ip::make_address_v4("239.255.0.71");
constexpr std::uint16_t k_test_port       = 17447;
constexpr unsigned k_test_ttl             = 1;

std::vector<std::byte> bytes_of(const std::string &s)
{
    std::vector<std::byte> out(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        out[i] = static_cast<std::byte>(s[i]);
    return out;
}

std::string str_of(std::span<const std::byte> b)
{
    std::string s(b.size(), '\0');
    for(std::size_t i = 0; i < b.size(); ++i)
        s[i] = static_cast<char>(std::to_integer<unsigned char>(b[i]));
    return s;
}

template<typename Pred>
void pump_until(::asio::io_context &io, Pred pred)
{
    auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

TEST_CASE("two udp_multicast_sockets on one group: bind + join run without error", "[datagram_socket][multicast]")
{
    ::asio::io_context io;
    pasio::udp_multicast_socket receiver{io, k_test_group, k_test_port, k_test_ttl};
    pasio::udp_multicast_socket sender{io, k_test_group, k_test_port, k_test_ttl};

    REQUIRE_FALSE(receiver.bind({::asio::ip::udp::v4(), k_test_port}));
    REQUIRE_FALSE(sender.bind({::asio::ip::udp::v4(), k_test_port}));
}

// Hidden [.multicast]: real same-host multicast loopback is environment-sensitive (a sandbox may
// drop IGMP-joined delivery). The construct/bind/join no-error proof above stays in the default set;
// this delivery proof runs on demand. It is NEVER weakened to a 127.0.0.1 unicast send — that would
// not exercise the join.
TEST_CASE("a sent multicast datagram is delivered to on_datagram with the source endpoint", "[.multicast]")
{
    ::asio::io_context io;
    pasio::udp_multicast_socket receiver{io, k_test_group, k_test_port, k_test_ttl};
    pasio::udp_multicast_socket sender{io, k_test_group, k_test_port, k_test_ttl};

    std::optional<std::string> got;
    std::optional<pasio::udp_multicast_socket::endpoint_type> from_ep;
    receiver.on_datagram(
            [&](const pasio::udp_multicast_socket::endpoint_type &from, std::span<const std::byte> b)
            {
                got     = str_of(b);
                from_ep = from;
            });

    REQUIRE_FALSE(receiver.bind({::asio::ip::udp::v4(), k_test_port}));
    REQUIRE_FALSE(sender.bind({::asio::ip::udp::v4(), k_test_port}));

    const std::string payload = "announce-hello";
    sender.send_multicast(bytes_of(payload));

    pump_until(io, [&] { return got.has_value(); });

    REQUIRE(got.has_value());
    REQUIRE(*got == payload);
    REQUIRE(from_ep.has_value());
    REQUIRE(from_ep->port() != 0);
    REQUIRE_FALSE(from_ep->address().is_unspecified());
}

}
