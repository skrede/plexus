// The host xTaskCreate records the receive task instead of running it, so each case that spawns one runs the recorded
// trampoline after the close, as the executor would see the task finish.

#include "lwip_rx_rig.h"
#include "host_tcp_listener.h"
#include "stalling_host_socket.h"

#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/endpoint.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <cstddef>
#include <optional>

using plexus::test::g_stall;
using plexus::test::host_tcp_socket;
using plexus::test::host_tcp_listener;
using plexus::test::stalling_host_socket;

namespace {

using stalling_transport  = plexus::freertos::lwip_transport<stalling_host_socket>;
using stalling_channel    = stalling_transport::channel_type;
using accepting_transport = plexus::freertos::lwip_transport<host_tcp_socket, host_tcp_listener, 1>;
using accepted_channel    = accepting_transport::channel_type;

constexpr std::array<std::byte, 4> k_payload{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};

void forget_spawned_task()
{
    plexus::freertos::detail::host_task_created_code = nullptr;
    plexus::freertos::detail::host_task_created_arg  = nullptr;
}

class stall_scope
{
public:
    stall_scope()
    {
        g_stall = true;
    }

    ~stall_scope()
    {
        g_stall = false;
    }

    stall_scope(const stall_scope &)            = delete;
    stall_scope &operator=(const stall_scope &) = delete;
};

// Runs before the executor is destroyed whether or not the case's checks held, so a spawned task always exits.
class task_finisher
{
public:
    task_finisher(std::unique_ptr<stalling_channel> &channel, plexus::freertos::freertos_executor &ex)
            : m_channel(channel)
            , m_ex(ex)
    {
    }

    ~task_finisher()
    {
        if(m_channel)
            m_channel->close();
        if(plexus::freertos::detail::host_task_created_code)
            plexus::freertos::detail::host_task_created_code(plexus::freertos::detail::host_task_created_arg);
        m_ex.drain();
        forget_spawned_task();
    }

    task_finisher(const task_finisher &)            = delete;
    task_finisher &operator=(const task_finisher &) = delete;

private:
    std::unique_ptr<stalling_channel> &m_channel;
    plexus::freertos::freertos_executor &m_ex;
};

template<typename Transport, typename Ready>
bool pump_until(Transport &transport, plexus::freertos::freertos_executor &ex, Ready ready)
{
    return plexus::test::poll_until(
            [&]
            {
                transport.poll();
                ex.drain();
            },
            ready);
}

void read_available(host_tcp_socket &peer, std::vector<std::byte> &received)
{
    std::array<std::byte, 256> buffer{};
    const std::size_t n = peer.recv(buffer);
    received.insert(received.end(), buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(n));
}

plexus::io::endpoint free_loopback_endpoint()
{
    host_tcp_listener probe;
    REQUIRE_FALSE(probe.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    return probe.local_endpoint();
}

}

TEST_CASE("lwip rx task: a send stalled under the task policy is retried by the transport's poll and flushes", "[seam][lwip_rx_task]")
{
    forget_spawned_task();
    const stall_scope stalled;
    host_tcp_listener listener;
    REQUIRE_FALSE(listener.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    plexus::freertos::freertos_executor ex;
    stalling_transport transport{ex};
    transport.use_rx_task(plexus::freertos::task_options{4096});
    std::unique_ptr<stalling_channel> channel;
    const task_finisher finisher{channel, ex};
    transport.on_dialed([&channel](std::unique_ptr<stalling_channel> ch, const plexus::io::endpoint &) { channel = std::move(ch); });

    transport.dial(listener.local_endpoint());
    REQUIRE(pump_until(transport, ex, [&channel] { return channel != nullptr; }));
    std::optional<host_tcp_socket> peer;
    REQUIRE(plexus::test::poll_until([&] { peer = listener.accept_one(); }, [&peer] { return peer.has_value(); }));

    const auto frame = plexus::test::on_wire(k_payload);
    channel->send(frame);
    g_stall = false;
    std::vector<std::byte> received;
    const bool flushed = plexus::test::poll_until(
            [&]
            {
                transport.poll();
                ex.drain();
                read_available(*peer, received);
            },
            [&] { return received.size() >= frame.size(); });

    CHECK(flushed);
    CHECK(received == frame);
}

TEST_CASE("lwip rx task: a channel accepted before the task policy is switched on keeps receiving from poll", "[seam][lwip_rx_task]")
{
    forget_spawned_task();
    plexus::freertos::freertos_executor ex;
    accepting_transport transport{ex};
    const auto address = free_loopback_endpoint();
    transport.listen(address);
    std::unique_ptr<accepted_channel> accepted;
    transport.on_accepted([&accepted](std::unique_ptr<accepted_channel> ch) { accepted = std::move(ch); });

    host_tcp_socket client;
    REQUIRE_FALSE(client.connect(address));
    REQUIRE(pump_until(transport, ex, [&accepted] { return accepted != nullptr; }));
    std::vector<std::byte> delivered;
    accepted->on_data([&delivered](std::span<const std::byte> bytes) { delivered.insert(delivered.end(), bytes.begin(), bytes.end()); });

    transport.use_rx_task(plexus::freertos::task_options{4096});
    const auto frame = plexus::test::on_wire(k_payload);
    client.send(frame);
    const bool received = pump_until(transport, ex, [&delivered] { return !delivered.empty(); });

    CHECK(received);
    CHECK(delivered == frame);
    CHECK(plexus::freertos::detail::host_task_created_arg == nullptr);
    accepted->close();
    ex.drain();
}
