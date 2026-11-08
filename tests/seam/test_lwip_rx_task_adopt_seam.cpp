// A connection the receive-task policy cannot serve is refused at adoption. The checks run before the cleanup, which
// closes whatever channel was handed out and runs any trampoline the host xTaskCreate recorded, so the executor is never
// destroyed under a live task.

#include "host_tcp_socket.h"
#include "host_tcp_listener.h"
#include "host_loopback_pair.h"
#include "scripted_dial_socket.h"

#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/freertos/detail/freertos_host_shim.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <vector>
#include <cstddef>
#include <utility>
#include <stdexcept>
#include <system_error>

using plexus::test::g_dial_script;
using plexus::test::host_tcp_socket;
using plexus::test::host_tcp_listener;
using plexus::test::scripted_dial_socket;

namespace {

namespace shim = plexus::freertos::detail;

using dial_transport   = plexus::freertos::lwip_transport<scripted_dial_socket>;
using dial_channel     = dial_transport::channel_type;
using accept_transport = plexus::freertos::lwip_transport<host_tcp_socket, host_tcp_listener, 1>;
using accept_channel   = accept_transport::channel_type;

const plexus::io::endpoint k_peer{"tcp", "10.0.0.1:7000"};

void forget_spawned_task()
{
    shim::host_task_created_code = nullptr;
    shim::host_task_created_arg  = nullptr;
}

// Each case starts from a fresh dial script and spawn record, and the spawn result is restored when it ends.
class spawn_scope
{
public:
    explicit spawn_scope(BaseType_t result)
    {
        g_dial_script = plexus::test::dial_script{};
        forget_spawned_task();
        shim::host_task_create_result = result;
    }

    ~spawn_scope()
    {
        shim::host_task_create_result = pdTRUE;
    }

    spawn_scope(const spawn_scope &)            = delete;
    spawn_scope &operator=(const spawn_scope &) = delete;
};

struct dial_record
{
    std::vector<std::unique_ptr<dial_channel>> dialed;
    std::vector<plexus::io::io_error> failed;
};

bool await_dial(dial_transport &transport, plexus::freertos::freertos_executor &ex, dial_record &out)
{
    transport.on_dialed([&out](std::unique_ptr<dial_channel> ch, const plexus::io::endpoint &) { out.dialed.push_back(std::move(ch)); });
    transport.on_dial_failed([&out](const plexus::io::endpoint &, plexus::io::io_error e) { out.failed.push_back(e); });
    transport.dial(k_peer);
    return plexus::test::poll_until(
            [&]
            {
                transport.poll();
                ex.drain();
            },
            [&out] { return !out.dialed.empty() || !out.failed.empty(); });
}

bool resolve_dial(dial_transport &transport, plexus::freertos::freertos_executor &ex, dial_record &out)
{
    transport.use_rx_task(plexus::freertos::task_options{4096});
    return await_dial(transport, ex, out);
}

void finish(std::vector<std::unique_ptr<dial_channel>> &dialed, plexus::freertos::freertos_executor &ex)
{
    for(auto &ch : dialed)
        ch->close();
    if(shim::host_task_created_code)
        shim::host_task_created_code(shim::host_task_created_arg);
    ex.drain();
    forget_spawned_task();
}

plexus::io::endpoint free_loopback_endpoint()
{
    host_tcp_listener probe;
    REQUIRE_FALSE(probe.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    return probe.local_endpoint();
}

}

TEST_CASE("lwip rx task: an accepted connection whose receive task cannot be created is refused and closed", "[seam][lwip_rx_task]")
{
    const spawn_scope failing{pdFALSE};
    plexus::freertos::freertos_executor ex;
    accept_transport transport{ex};
    transport.use_rx_task(plexus::freertos::task_options{4096});
    const auto address = free_loopback_endpoint();
    transport.listen(address);
    std::unique_ptr<accept_channel> accepted;
    transport.on_accepted([&accepted](std::unique_ptr<accept_channel> ch) { accepted = std::move(ch); });

    host_tcp_socket client;
    REQUIRE_FALSE(client.connect(address));
    std::array<std::byte, 16> sink{};
    const bool refused = plexus::test::poll_until(
            [&]
            {
                transport.poll();
                ex.drain();
                client.recv(sink);
            },
            [&client] { return client.closed(); });

    CHECK(accepted == nullptr);
    CHECK(refused);
    if(accepted)
        accepted->close();
    ex.drain();
}

TEST_CASE("lwip rx task: a dial whose receive task cannot be created fails and closes its socket", "[seam][lwip_rx_task]")
{
    const spawn_scope failing{pdFALSE};
    g_dial_script.outcome = std::error_code{};
    plexus::freertos::freertos_executor ex;
    dial_transport transport{ex};
    dial_record out;

    CHECK(resolve_dial(transport, ex, out));
    CHECK(out.dialed.empty());
    CHECK(out.failed == std::vector{plexus::io::io_error::unknown});
    CHECK(g_dial_script.closes == 1);
    finish(out.dialed, ex);
}

TEST_CASE("lwip rx task: a dial whose receive timeout the socket rejects fails without a receive task", "[seam][lwip_rx_task]")
{
    const spawn_scope spawning{pdTRUE};
    g_dial_script.outcome                  = std::error_code{};
    g_dial_script.receive_timeout_accepted = false;
    plexus::freertos::freertos_executor ex;
    dial_transport transport{ex};
    dial_record out;

    CHECK(resolve_dial(transport, ex, out));
    CHECK(out.dialed.empty());
    CHECK(out.failed == std::vector{plexus::io::io_error::unknown});
    CHECK(shim::host_task_created_arg == nullptr);
    CHECK(g_dial_script.closes == 1);
    finish(out.dialed, ex);
}

TEST_CASE("lwip rx task: a receive timeout set out of range is refused by use_rx_task and a later dial still resolves", "[seam][lwip_rx_task]")
{
    const spawn_scope spawning{pdTRUE};
    g_dial_script.outcome = std::error_code{};
    plexus::freertos::freertos_executor ex;
    dial_transport transport{ex};
    dial_record out;
    plexus::freertos::rx_task_options opts{plexus::freertos::task_options{4096}};
    opts.recv_timeout = std::chrono::milliseconds{0};

    CHECK_THROWS_AS(transport.use_rx_task(opts), std::runtime_error);
    bool resolved = false;
    CHECK_NOTHROW(resolved = await_dial(transport, ex, out));
    CHECK(resolved);
    CHECK(out.dialed.size() == 1);
    CHECK(shim::host_task_created_arg == nullptr);
    finish(out.dialed, ex);
}
