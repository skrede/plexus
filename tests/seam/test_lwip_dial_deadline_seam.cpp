#include "host_tcp_socket.h"
#include "host_loopback_pair.h"
#include "scripted_dial_socket.h"

#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/detail/node_internals.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

using namespace std::chrono_literals;
using plexus::test::g_dial_script;
using plexus::test::host_tcp_socket;
using plexus::test::scripted_dial_socket;

namespace {

template<typename Socket, std::size_t MaxClients = 1>
struct dial_rig
{
    using transport_type = plexus::freertos::lwip_transport<Socket, plexus::freertos::detail::null_acceptor<Socket>, MaxClients>;
    using channel_type   = typename transport_type::channel_type;

    plexus::freertos::freertos_executor ex;
    transport_type transport;
    std::chrono::steady_clock::time_point failed_at;
    std::vector<std::pair<plexus::io::endpoint, plexus::io::io_error>> failed;
    std::vector<std::pair<std::unique_ptr<channel_type>, plexus::io::endpoint>> dialed;

    dial_rig()
            : ex()
            , transport(ex)
            , failed_at()
            , failed()
            , dialed()
    {
        transport.on_dialed([this](std::unique_ptr<channel_type> ch, const plexus::io::endpoint &ep) { dialed.emplace_back(std::move(ch), ep); });
        transport.on_dial_failed(
                [this](const plexus::io::endpoint &ep, plexus::io::io_error e)
                {
                    failed.emplace_back(ep, e);
                    failed_at = std::chrono::steady_clock::now();
                });
    }

    void pump()
    {
        transport.poll();
        ex.drain();
    }

    template<typename Ready>
    bool pump_until(Ready ready, std::chrono::milliseconds budget = 2s)
    {
        return plexus::test::poll_until([this] { pump(); }, ready, budget);
    }
};

const plexus::io::endpoint k_first{"tcp", "10.0.0.1:7000"};
const plexus::io::endpoint k_second{"tcp", "10.0.0.2:7000"};

}

TEST_CASE("lwip dial deadline: a dial still pending when the node's dial timeout elapses fails with timed_out and closes its socket", "[seam][lwip_dial]")
{
    g_dial_script = plexus::test::dial_script{};
    dial_rig<scripted_dial_socket> rig;
    plexus::detail::apply_dial_timeout(rig.transport, 30ms);

    const auto dialed_at = std::chrono::steady_clock::now();
    rig.transport.dial(k_first);
    REQUIRE(rig.failed.empty());
    REQUIRE(rig.pump_until([&rig] { return !rig.failed.empty(); }));

    REQUIRE(rig.failed.size() == 1);
    CHECK(rig.failed[0].first == k_first);
    CHECK(rig.failed[0].second == plexus::io::io_error::timed_out);
    CHECK(rig.failed_at - dialed_at >= 30ms);
    CHECK(rig.dialed.empty());
    CHECK(g_dial_script.closes == 1);
}

TEST_CASE("lwip dial deadline: a dial timeout frees the pending slot for the next dial", "[seam][lwip_dial]")
{
    g_dial_script = plexus::test::dial_script{};
    dial_rig<scripted_dial_socket, 1> rig;
    plexus::detail::apply_dial_timeout(rig.transport, 30ms);

    rig.transport.dial(k_first);
    rig.transport.dial(k_second);
    rig.ex.drain();
    REQUIRE(rig.failed.size() == 1);
    CHECK(rig.failed[0].first == k_second);
    CHECK(rig.failed[0].second == plexus::io::io_error::would_block);

    REQUIRE(rig.pump_until([&rig] { return rig.failed.size() == 2; }));
    CHECK(rig.failed[1].first == k_first);
    CHECK(rig.failed[1].second == plexus::io::io_error::timed_out);

    const std::uint32_t probes = g_dial_script.probes;
    rig.transport.dial(k_second);
    rig.ex.drain();
    CHECK(rig.failed.size() == 2);
    rig.pump();
    CHECK(rig.failed.size() == 2);
    CHECK(g_dial_script.probes == probes + 1);
}

TEST_CASE("lwip dial deadline: a dial that resolves before its dial timeout is delivered", "[seam][lwip_dial]")
{
    g_dial_script = plexus::test::dial_script{};
    dial_rig<scripted_dial_socket> rig;
    plexus::detail::apply_dial_timeout(rig.transport, 2s);

    rig.transport.dial(k_first);
    for(std::uint32_t i = 0; i < 3; ++i)
        rig.pump();
    REQUIRE(rig.dialed.empty());
    REQUIRE(rig.failed.empty());

    g_dial_script.outcome = std::error_code{};
    rig.pump();
    REQUIRE(rig.dialed.size() == 1);
    CHECK(rig.dialed[0].second == k_first);
    CHECK(rig.failed.empty());
}

TEST_CASE("lwip dial deadline: a zero dial timeout leaves a dial pending", "[seam][lwip_dial]")
{
    g_dial_script = plexus::test::dial_script{};
    dial_rig<scripted_dial_socket> rig;
    plexus::detail::apply_dial_timeout(rig.transport, 30ms);
    plexus::detail::apply_dial_timeout(rig.transport, 0ns);

    rig.transport.dial(k_first);
    std::uint32_t polls = 0;
    const bool reported = plexus::test::poll_until(
            [&]
            {
                rig.pump();
                ++polls;
            },
            [&rig] { return !rig.failed.empty() || !rig.dialed.empty(); }, 60ms);

    CHECK_FALSE(reported);
    CHECK(polls > 0);
    CHECK(g_dial_script.probes == polls);
    CHECK(g_dial_script.closes == 0);
}

TEST_CASE("lwip dial errors: a connect lwIP aborted after its SYN retransmissions reports timed_out", "[seam][lwip_dial]")
{
    g_dial_script         = plexus::test::dial_script{};
    g_dial_script.outcome = std::make_error_code(std::errc::connection_aborted);
    dial_rig<scripted_dial_socket> rig;

    rig.transport.dial(k_first);
    rig.pump();
    REQUIRE(rig.failed.size() == 1);
    CHECK(rig.failed[0].first == k_first);
    CHECK(rig.failed[0].second == plexus::io::io_error::timed_out);
    CHECK(rig.dialed.empty());
}

TEST_CASE("lwip dial errors: a dial to an address that is not dotted IPv4 is refused off the dial call", "[seam][lwip_dial]")
{
    dial_rig<host_tcp_socket> rig;
    const plexus::io::endpoint named{"tcp", "sensor-hub.local:7447"};

    rig.transport.dial(named);
    REQUIRE(rig.failed.empty());
    rig.ex.drain();
    REQUIRE(rig.failed.size() == 1);
    CHECK(rig.failed[0].first == named);
    CHECK(rig.failed[0].second == plexus::io::io_error::unknown);
    CHECK(rig.dialed.empty());
}
