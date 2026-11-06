#include "test_tls_large_payload_common.h"

#include "support/fd_exhaustion.h"

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_listener.h"
#include "plexus/tls/tls_credential.h"

#include "plexus/io/io_error.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <cstdint>

using tls_large_payload_fixture::make_cred;
using tls_large_payload_fixture::identity_fixture;

// The client socket is open before the table fills, so only the listener's accept needs a descriptor; the accepted
// channel reaches on_accepted once its server handshake completes.
TEST_CASE("tls listener accept retry: a tls listener whose accept fails with EMFILE accepts again once descriptors free", "[tls][accept]")
{
    identity_fixture server_id("srv_retry");
    identity_fixture client_id("cli_retry");
    const auto server_cred = make_cred(server_id, client_id.digest);
    const auto client_cred = make_cred(client_id, server_id.digest);

    ::asio::io_context io;
    plexus::tls::tls_listener listener{io, server_cred};
    std::unique_ptr<plexus::tls::tls_channel> server;
    std::int32_t accepted = 0;
    std::int32_t errors   = 0;
    listener.on_accepted(
            [&](std::unique_ptr<plexus::tls::tls_channel> ch)
            {
                server = std::move(ch);
                ++accepted;
            });
    listener.on_error([&](plexus::io::io_error) { ++errors; });
    listener.start({"tcp", "127.0.0.1:0"});

    plexus::tls::tls_channel client{io, client_cred};
    client.socket().open(::asio::ip::tcp::v4());
    const ::asio::ip::tcp::endpoint target(::asio::ip::make_address("127.0.0.1"), listener.port());

    plexus::testing::fd_exhaustion table;
    REQUIRE(table.full());
    client.socket().connect(target);
    REQUIRE(plexus::testing::pump_until(io, [&] { return errors > 0; }, std::chrono::seconds(5)));
    table.release();
    client.start_client_handshake("");

    plexus::testing::pump_until(io, [&] { return accepted > 0; }, std::chrono::milliseconds(1500));
    REQUIRE(accepted == 1);
}
