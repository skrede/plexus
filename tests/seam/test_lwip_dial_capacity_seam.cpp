// A dialed channel takes a slot in the same bounded view set as an accepted one; a channel dialed past a
// full set would never be polled, so the dial is refused instead.

#include "host_tcp_socket.h"
#include "host_tcp_listener.h"
#include "host_loopback_pair.h"

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/io_error.h"
#include "plexus/io/endpoint.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

using plexus::test::host_tcp_socket;
using plexus::test::host_tcp_listener;
using lwip_channel = plexus::freertos::lwip_channel<host_tcp_socket>;

template<std::size_t MaxClients>
using accept_transport = plexus::freertos::lwip_transport<host_tcp_socket, host_tcp_listener, MaxClients>;

namespace {

struct dial_outcomes
{
    std::vector<std::unique_ptr<lwip_channel>> dialed;
    std::vector<std::pair<plexus::io::endpoint, plexus::io::io_error>> failed;
};

plexus::io::endpoint free_loopback_port()
{
    host_tcp_listener probe;
    REQUIRE_FALSE(probe.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    return probe.local_endpoint();
}

template<std::size_t MaxClients>
void accept_one_client(accept_transport<MaxClients> &transport, host_tcp_socket &client, std::unique_ptr<lwip_channel> &accepted)
{
    const plexus::io::endpoint address = free_loopback_port();
    transport.listen(address);
    transport.on_accepted([&accepted](std::unique_ptr<lwip_channel> ch) { accepted = std::move(ch); });
    client.connect(address);
    plexus::test::poll_until([&transport] { transport.poll(); }, [&accepted] { return accepted != nullptr; });
}

template<std::size_t MaxClients>
void record_dials(accept_transport<MaxClients> &transport, dial_outcomes &out)
{
    transport.on_dialed([&out](std::unique_ptr<lwip_channel> ch, const plexus::io::endpoint &) { out.dialed.push_back(std::move(ch)); });
    transport.on_dial_failed([&out](const plexus::io::endpoint &ep, plexus::io::io_error e) { out.failed.emplace_back(ep, e); });
}

}

TEST_CASE("lwip_transport refuses a dial while its view set is full and reports the dial failed", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    accept_transport<1> transport{ex};
    host_tcp_socket client;
    std::unique_ptr<lwip_channel> accepted;
    accept_one_client(transport, client, accepted);
    REQUIRE(accepted != nullptr);

    host_tcp_listener target;
    REQUIRE_FALSE(target.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    dial_outcomes out;
    record_dials(transport, out);
    transport.dial(target.local_endpoint());

    REQUIRE(out.dialed.empty());
    REQUIRE(out.failed.size() == 1);
    REQUIRE(out.failed[0].first == target.local_endpoint());
    REQUIRE(out.failed[0].second == plexus::io::io_error::would_block);
    REQUIRE_FALSE(target.accept_one().has_value());

    std::vector<std::byte> received;
    accepted->on_data([&received](std::span<const std::byte> frame) { received.assign(frame.begin(), frame.end()); });
    const std::byte payload[] = {std::byte{0xA5}, std::byte{0x5A}};
    client.send(plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0},
                                           payload));
    REQUIRE(plexus::test::poll_until([&transport] { transport.poll(); }, [&received] { return !received.empty(); }));
}

TEST_CASE("lwip_transport dials normally while a view slot is free", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    accept_transport<2> transport{ex};
    host_tcp_socket client;
    std::unique_ptr<lwip_channel> accepted;
    accept_one_client(transport, client, accepted);
    REQUIRE(accepted != nullptr);

    host_tcp_listener target;
    REQUIRE_FALSE(target.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    dial_outcomes out;
    record_dials(transport, out);
    transport.dial(target.local_endpoint());

    REQUIRE(out.failed.empty());
    REQUIRE(out.dialed.size() == 1);
    REQUIRE(out.dialed[0] != nullptr);
}
