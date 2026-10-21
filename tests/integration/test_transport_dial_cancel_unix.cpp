#include "transport_dial_cancel_common.h"

#include "plexus/asio/unix_channel.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/local/stream_protocol.hpp>

#include <string>
#include <filesystem>
#include <system_error>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using namespace transport_dial_cancel;
using local = ::asio::local::stream_protocol;

namespace {

struct socket_dir
{
    std::filesystem::path dir;
    std::string path;

    socket_dir()
            : dir(plexus::testing::make_temp_dir("pxdc-"))
            , path((dir / "s").string())
    {
    }

    ~socket_dir()
    {
        plexus::testing::remove_socket_path(path);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
};

}

// A unix connect to a listener with room completes inside dial() and only its completion is queued, so the transport
// closes with the dial's outcome not yet handled.
TEST_CASE("transport_dial_cancel: a unix dial whose connect completion is still queued when its transport closes reports one aborted failure and is never dialed",
          "[integration][transport][unix][dial_cancel]")
{
    socket_dir sock;
    ::asio::io_context io;
    local::acceptor peer{io, local::endpoint{sock.path}};
    outcome<pasio::unix_channel> seen;
    pasio::unix_transport transport{io};
    seen.record(transport);

    transport.dial(pio::endpoint{"unix", sock.path});
    transport.close();
    drain(io);

    CHECK(seen.dialed.empty());
    REQUIRE(seen.failures.size() == 1);
    CHECK(seen.failures.front() == pio::io_error::operation_aborted);
}

TEST_CASE("transport_accept_cancel: a unix accept completed but not yet handled when its transport closes is dropped", "[integration][transport][unix][accept_cancel]")
{
    socket_dir sock;
    ::asio::io_context io;
    outcome<pasio::unix_channel> seen;
    pasio::unix_transport transport{io};
    seen.record(transport);
    transport.listen(pio::endpoint{"unix", sock.path});
    local::socket first{io};
    local::socket second{io};
    first.connect(local::endpoint{sock.path});
    second.connect(local::endpoint{sock.path});

    // Handling the first accept re-arms the accept, which takes the second connection off the backlog at once and
    // queues its completion.
    io.poll_one();
    REQUIRE(seen.accepted.size() == 1);
    transport.close();
    drain(io);

    CHECK(seen.accepted.size() == 1);
    CHECK(closed_by_peer(second));
}
