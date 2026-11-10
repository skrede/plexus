#include "support/fd_exhaustion.h"

#include "plexus/asio/asio_channel.h"
#include "plexus/asio/unix_channel.h"
#include "plexus/asio/asio_listener.h"
#include "plexus/asio/unix_listener.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>
#include <asio/local/stream_protocol.hpp>

#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <cstdint>
#include <cstdlib>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

namespace {

constexpr std::chrono::seconds k_error_bound{5};
constexpr std::chrono::milliseconds k_recovery_bound{1500};

struct temp_dir
{
    std::string dir;
    std::string path;

    temp_dir()
    {
        char tmpl[]      = "/tmp/pxretry-XXXXXX";
        const char *made = ::mkdtemp(tmpl);
        dir              = made ? made : "";
        path             = dir + "/s";
    }

    ~temp_dir()
    {
        ::unlink(path.c_str());
        ::rmdir(dir.c_str());
    }
};

// The client socket is open before the table fills, so connecting needs no descriptor; the listener's accept is the
// first allocation that meets EMFILE.
template<typename Channel, typename Listener, typename Connect>
std::int32_t accepts_after_exhaustion(::asio::io_context &io, Listener &listener, Connect connect)
{
    std::unique_ptr<Channel> server;
    std::int32_t accepted = 0;
    std::int32_t errors   = 0;
    listener.on_accepted(
            [&](std::unique_ptr<Channel> ch)
            {
                server = std::move(ch);
                ++accepted;
            });
    listener.on_error([&](pio::io_error) { ++errors; });

    plexus::testing::fd_exhaustion table;
    REQUIRE(table.full());
    connect();
    REQUIRE(plexus::testing::pump_until(io, [&] { return errors > 0; }, k_error_bound));
    table.release();

    plexus::testing::pump_until(io, [&] { return accepted > 0; }, k_recovery_bound);
    return accepted;
}

::asio::ip::tcp::endpoint loopback(const pasio::asio_listener &listener)
{
    return {::asio::ip::make_address("127.0.0.1"), listener.port()};
}

}

TEST_CASE("listener accept retry: an asio listener whose accept fails with EMFILE accepts again once descriptors free", "[integration][asio][accept]")
{
    ::asio::io_context io;
    pasio::asio_listener listener{io};
    listener.start({"tcp", "127.0.0.1:0"});
    ::asio::ip::tcp::socket client{io};
    client.open(::asio::ip::tcp::v4());

    const auto target = loopback(listener);
    REQUIRE(accepts_after_exhaustion<pasio::asio_channel>(io, listener, [&] { client.connect(target); }) == 1);
}

TEST_CASE("listener accept retry: a unix listener whose accept fails with EMFILE accepts again once descriptors free", "[integration][unix][accept]")
{
    temp_dir t;
    REQUIRE_FALSE(t.dir.empty());
    ::asio::io_context io;
    pasio::unix_listener listener{io};
    listener.start(pio::endpoint{"unix", t.path});
    ::asio::local::stream_protocol::socket client{io};
    client.open();

    const ::asio::local::stream_protocol::endpoint target(t.path);
    REQUIRE(accepts_after_exhaustion<pasio::unix_channel>(io, listener, [&] { client.connect(target); }) == 1);
}

TEST_CASE("listener accept retry: stopping an asio listener during an armed accept backoff destroys it cleanly", "[integration][asio][accept]")
{
    ::asio::io_context io;
    auto listener       = std::make_unique<pasio::asio_listener>(io);
    std::int32_t errors = 0;
    listener->on_error([&](pio::io_error) { ++errors; });
    listener->start({"tcp", "127.0.0.1:0"});
    ::asio::ip::tcp::socket client{io};
    client.open(::asio::ip::tcp::v4());
    const auto target = loopback(*listener);

    {
        plexus::testing::fd_exhaustion table;
        REQUIRE(table.full());
        client.connect(target);
        REQUIRE(plexus::testing::pump_until(io, [&] { return errors > 0; }, k_error_bound));
    }

    listener->stop();
    io.restart();
    while(io.poll() != 0)
    {
    }
    REQUIRE(io.stopped());
    listener.reset();
    io.restart();
    io.poll();
}
