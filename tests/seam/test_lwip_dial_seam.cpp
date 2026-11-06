#include "host_loopback_pair.h"
#include "scripted_dial_socket.h"

#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/freertos_executor.h"

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

using plexus::test::g_dial_script;
using plexus::test::scripted_dial_socket;

template<std::size_t MaxClients>
using dial_transport = plexus::freertos::lwip_transport<scripted_dial_socket, plexus::freertos::detail::null_acceptor<scripted_dial_socket>, MaxClients>;
using dial_channel   = dial_transport<1>::channel_type;

namespace {

struct dial_record
{
    std::vector<std::unique_ptr<dial_channel>> dialed;
    std::vector<std::pair<plexus::io::endpoint, plexus::io::io_error>> failed;
};

template<std::size_t MaxClients>
void record(dial_transport<MaxClients> &transport, dial_record &out)
{
    transport.on_dialed([&out](std::unique_ptr<dial_channel> ch, const plexus::io::endpoint &) { out.dialed.push_back(std::move(ch)); });
    transport.on_dial_failed([&out](const plexus::io::endpoint &ep, plexus::io::io_error e) { out.failed.emplace_back(ep, e); });
}

void tick_every_ms(plexus::freertos::freertos_timer &timer, std::uint32_t &fires)
{
    timer.expires_after(std::chrono::milliseconds{1});
    timer.async_wait(
            [&timer, &fires](std::error_code ec)
            {
                if(ec)
                    return;
                ++fires;
                tick_every_ms(timer, fires);
            });
}

const plexus::io::endpoint k_first{"tcp", "10.0.0.1:7000"};
const plexus::io::endpoint k_second{"tcp", "10.0.0.2:7000"};

}

TEST_CASE("lwip dial: a dial to a host that never answers returns at once and the executor keeps running", "[seam][lwip_dial]")
{
    g_dial_script = plexus::test::dial_script{};
    plexus::freertos::freertos_executor ex;
    dial_transport<1> transport{ex};
    dial_record out;
    record(transport, out);
    std::uint32_t fires = 0;
    plexus::freertos::freertos_timer timer{ex};
    tick_every_ms(timer, fires);

    transport.dial(k_first);
    REQUIRE(out.dialed.empty());
    REQUIRE(out.failed.empty());

    std::uint32_t polls = 0;
    const bool ticked   = plexus::test::poll_until(
            [&]
            {
                transport.poll();
                ++polls;
                ex.drain();
            },
            [&fires] { return fires >= 20; });

    CHECK(ticked);
    CHECK(out.dialed.empty());
    CHECK(out.failed.empty());
    CHECK(g_dial_script.probes == polls);
}

TEST_CASE("lwip dial: a dial while every pending slot is taken is refused off the dial call", "[seam][lwip_dial]")
{
    g_dial_script = plexus::test::dial_script{};
    plexus::freertos::freertos_executor ex;
    dial_transport<1> transport{ex};
    dial_record out;
    record(transport, out);

    transport.dial(k_first);
    transport.dial(k_second);
    REQUIRE(out.dialed.empty());
    REQUIRE(out.failed.empty());

    transport.poll();
    ex.drain();
    REQUIRE(out.failed.size() == 1);
    CHECK(out.failed[0].first == k_second);
    CHECK(out.failed[0].second == plexus::io::io_error::would_block);
    CHECK(out.dialed.empty());
    CHECK(g_dial_script.probes == 1);
}

TEST_CASE("lwip dial: closing the transport aborts a pending dial off the close call", "[seam][lwip_dial]")
{
    g_dial_script = plexus::test::dial_script{};
    plexus::freertos::freertos_executor ex;
    dial_transport<1> transport{ex};
    dial_record out;
    record(transport, out);

    transport.dial(k_first);
    REQUIRE(out.dialed.empty());
    transport.close();
    REQUIRE(out.failed.empty());
    CHECK(g_dial_script.closes == 1);

    ex.drain();
    REQUIRE(out.failed.size() == 1);
    CHECK(out.failed[0].first == k_first);
    CHECK(out.failed[0].second == plexus::io::io_error::operation_aborted);
    CHECK(out.dialed.empty());
}

TEST_CASE("lwip dial: a dial that completes while the view set is full fails and closes its socket", "[seam][lwip_dial]")
{
    g_dial_script         = plexus::test::dial_script{};
    g_dial_script.outcome = std::error_code{};
    plexus::freertos::freertos_executor ex;
    dial_transport<1> transport{ex};
    dial_record out;
    record(transport, out);

    transport.dial(k_first);
    REQUIRE(out.dialed.empty());
    transport.poll();
    ex.drain();
    REQUIRE(out.dialed.size() == 1);

    transport.dial(k_second);
    transport.poll();
    ex.drain();
    REQUIRE(out.failed.size() == 1);
    CHECK(out.failed[0].first == k_second);
    CHECK(out.failed[0].second == plexus::io::io_error::would_block);
    CHECK(out.dialed.size() == 1);
    CHECK(g_dial_script.closes == 1);
}
