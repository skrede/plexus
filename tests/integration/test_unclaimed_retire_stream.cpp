#include "test_unclaimed_retire_common.h"
#if defined(PLEXUS_HAVE_CRYPTO)
    #include "tls_retire_drain_common.h"
#endif
#if !defined(_WIN32)
    #include "openpty_compat.h"
#endif

#include "plexus/asio/asio_listener.h"
#include "plexus/asio/asio_transport.h"
#if !defined(_WIN32)
    #include "plexus/asio/unix_listener.h"
    #include "plexus/asio/serial_transport.h"
#endif

#if defined(PLEXUS_HAVE_CRYPTO)
    #include "plexus/tls/tls_channel.h"
    #include "plexus/tls/tls_listener.h"
    #include "plexus/tls/tls_transport.h"
#endif

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <asio/ip/tcp.hpp>
#if !defined(_WIN32)
    #include <asio/local/stream_protocol.hpp>

    #include <fcntl.h>
    #include <unistd.h>
#endif

#include <string>
#include <filesystem>
#include <system_error>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using namespace unclaimed_retire;
using tcp = ::asio::ip::tcp;

TEST_CASE("unclaimed_retire a tcp transport with no consumer keeps an accepted channel until its on_closed", "[integration][unclaimed_retire][tcp]")
{
    ::asio::io_context io;
    pasio::asio_transport transport{io};
    transport.listen({"tcp", "127.0.0.1:0"});

    CHECK(peer_sees_close<tcp::socket>(io, tcp::endpoint{::asio::ip::make_address("127.0.0.1"), transport.port()}));
}

TEST_CASE("unclaimed_retire a tcp transport with no consumer keeps a dialed channel until its on_closed", "[integration][unclaimed_retire][tcp]")
{
    ::asio::io_context io;
    pasio::asio_transport transport{io};

    CHECK(tcp_dial_sees_close(io, transport));
}

TEST_CASE("unclaimed_retire a tcp listener with no consumer keeps an accepted channel until its on_closed", "[integration][unclaimed_retire][tcp]")
{
    ::asio::io_context io;
    pasio::asio_listener listener{io};
    listener.start({"tcp", "127.0.0.1:0"});

    CHECK(peer_sees_close<tcp::socket>(io, tcp::endpoint{::asio::ip::make_address("127.0.0.1"), listener.port()}));
}

#if !defined(_WIN32)
TEST_CASE("unclaimed_retire a unix listener with no consumer keeps an accepted channel until its on_closed", "[integration][unclaimed_retire][unix]")
{
    const auto dir  = plexus::testing::make_temp_dir("pxur-");
    const auto path = (dir / "s").string();
    {
        ::asio::io_context io;
        pasio::unix_listener listener{io};
        listener.start({"unix", path});

        CHECK(peer_sees_close<::asio::local::stream_protocol::socket>(io, ::asio::local::stream_protocol::endpoint{path}));
    }
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

namespace {

struct pty_line
{
    int master;
    int slave;
    std::string path;

    pty_line()
            : master(-1)
            , slave(-1)
            , path()
    {
        REQUIRE(::openpty(&master, &slave, nullptr, nullptr, nullptr) == 0);
        path = ::ttyname(slave);
        (void)::fcntl(master, F_SETFL, ::fcntl(master, F_GETFL) | O_NONBLOCK);
    }

    ~pty_line()
    {
        ::close(master);
        ::close(slave);
    }

    pty_line(const pty_line &)            = delete;
    pty_line &operator=(const pty_line &) = delete;

    void feed() const
    {
        (void)::write(master, k_bytes.data(), k_bytes.size());
    }
};

}

TEST_CASE("unclaimed_retire a serial transport with no consumer keeps a listened channel until its on_closed", "[integration][unclaimed_retire][serial]")
{
    pty_line line;
    ::asio::io_context io;
    pasio::serial_transport transport{io};
    bool failed = false;
    transport.on_error([&failed](pio::io_error) { failed = true; });
    line.feed();
    transport.listen({"serial", line.path + "@115200"});

    run_window(io, [&line] { line.feed(); });
    CHECK_FALSE(failed);
}

TEST_CASE("unclaimed_retire a serial transport with no consumer keeps a dialed channel until its on_closed", "[integration][unclaimed_retire][serial]")
{
    pty_line line;
    ::asio::io_context io;
    pasio::serial_transport transport{io};
    bool failed = false;
    transport.on_dial_failed([&failed](const pio::endpoint &, pio::io_error) { failed = true; });
    line.feed();
    transport.dial({"serial", line.path + "@115200"});

    run_window(io, [&line] { line.feed(); });
    CHECK_FALSE(failed);
}
#endif

#if defined(PLEXUS_HAVE_CRYPTO)
namespace ptls = plexus::tls;

TEST_CASE("unclaimed_retire a tls transport with no consumer keeps an accepted channel until its on_closed", "[integration][unclaimed_retire][tls]")
{
    tls_retire_drain::mutual_credentials creds;
    ::asio::io_context io;
    ptls::tls_transport server{io, creds.server};
    ptls::tls_transport client{io, creds.client};
    holder<ptls::tls_channel> held;

    run_accept_unclaimed(io, server, client, held, "tls");
    REQUIRE(held.channel);
    CHECK(held.closed);
}

TEST_CASE("unclaimed_retire a tls transport with no consumer keeps a dialed channel until its on_closed", "[integration][unclaimed_retire][tls]")
{
    tls_retire_drain::mutual_credentials creds;
    ::asio::io_context io;
    ptls::tls_transport server{io, creds.server};
    ptls::tls_transport client{io, creds.client};
    holder<ptls::tls_channel> held;

    run_dial_unclaimed(io, client, server, held, "tls");
    REQUIRE(held.channel);
    CHECK(held.closed);
}

TEST_CASE("unclaimed_retire a tls listener with no consumer keeps an accepted channel until its on_closed", "[integration][unclaimed_retire][tls]")
{
    tls_retire_drain::mutual_credentials creds;
    ::asio::io_context io;
    ptls::tls_listener listener{io, creds.server};
    ptls::tls_transport client{io, creds.client};
    holder<ptls::tls_channel> held;
    client.on_dialed([&held](std::unique_ptr<ptls::tls_channel> ch, const pio::endpoint &) { held.take(std::move(ch)); });
    listener.start({"tls", "127.0.0.1:0"});
    client.dial({"tls", loopback(listener.port())});

    run_window(io, [&held] { held.feed(); });
    REQUIRE(held.channel);
    CHECK(held.closed);
}
#endif
