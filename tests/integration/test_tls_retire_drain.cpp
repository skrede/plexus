#include "tls_retire_drain_common.h"
#include "drain_after_destroy_common.h"

#include "support/alloc_watch.h"
#include "support/retire_drain.h"

#include "plexus/tls/tls_channel.h"

#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace ptls = plexus::tls;
namespace pio  = plexus::io;

using namespace drain_after_destroy;
using namespace tls_retire_drain;
using plexus::testing::retire_and_drain;

namespace {

constexpr std::size_t k_tls_engine_buffer = 17u * 1024u;

// Both ends handshaken over one loopback pair; the client is the peer the server channel reads from.
struct connected_tls
{
    tls_pair h;
    std::unique_ptr<ptls::tls_channel> server;
    ptls::tls_channel client;

    explicit connected_tls(const mutual_credentials &creds)
            : h()
            , server(std::make_unique<ptls::tls_channel>(h.io, std::move(h.raw_server), creds.server))
            , client(h.io, std::move(h.raw_client), creds.client)
    {
        bool server_ready = false;
        bool client_ready = false;
        server->start_server_handshake([&] { server_ready = true; });
        client.start_client_handshake("", [&] { client_ready = true; });
        REQUIRE(poll_one_until(h.io, [&] { return server_ready && client_ready; }));
    }
};

// The server handshake waits for a ClientHello the raw peer never sends, so it is pending at the retire.
struct pending_handshake
{
    tls_pair h;
    bool ready;
    std::unique_ptr<ptls::tls_channel> server;

    explicit pending_handshake(const ptls::tls_credential &cred)
            : h()
            , ready(false)
            , server(std::make_unique<ptls::tls_channel>(h.io, std::move(h.raw_server), cred))
    {
        server->start_server_handshake([this] { ready = true; });
        h.io.poll();
    }
};

}

TEST_CASE("tls_retire_drain a frame delivery queued before the channel closed is not handed to the consumer", "[integration][tls][teardown]")
{
    const mutual_credentials creds;
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        connected_tls t{creds};
        std::int32_t delivered = 0;
        t.server->on_data([&](std::span<const std::byte>) { ++delivered; });
        t.client.send(two_frames());
        REQUIRE(poll_one_until(t.h.io, [&] { return delivered > 0; }));
        const auto outcome = retire_and_drain(t.server, [&] { drain(t.h.io); });
        REQUIRE(outcome.erased == 1);
        REQUIRE(delivered == 1);
    }
}

TEST_CASE("tls_retire_drain a channel that failed before it was retired is still erased", "[integration][tls][teardown]")
{
    const mutual_credentials creds;
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        connected_tls t{creds};
        bool errored = false;
        t.server->on_error([&](pio::io_error) { errored = true; });
        t.client.close();
        REQUIRE(poll_one_until(t.h.io, [&] { return errored; }));
        t.h.io.poll();
        const auto outcome = retire_and_drain(t.server, [&] { drain(t.h.io); });
        REQUIRE(outcome.erased == 1);
        REQUIRE(outcome.empty);
    }
}

TEST_CASE("tls_retire_drain a handshake pending when the channel is retired touches nothing", "[integration][tls][teardown]")
{
    const identity server_id("srv");
    const auto cred = server_id.credential(server_id.digest);
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        pending_handshake p{cred};
        const auto outcome = retire_and_drain(p.server, [&] { drain(p.h.io); });
        REQUIRE(outcome.erased == 1);
        REQUIRE_FALSE(p.ready);
    }
}

TEST_CASE("tls_retire_drain the TLS engine buffers are freed only after the aborted operation drained", "[integration][tls][teardown]")
{
    const identity server_id("srv");
    const auto cred = server_id.credential(server_id.digest);
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        plexus::testing::watch_next_alloc(k_tls_engine_buffer);
        pending_handshake p{cred};
        REQUIRE(plexus::testing::watched_allocated());
        bool freed_at_close = true;
        const auto outcome  = retire_and_drain(p.server,
                                               [&]
                                               {
                                                  freed_at_close = plexus::testing::watched_freed();
                                                  drain(p.h.io);
                                               });
        REQUIRE(outcome.erased == 1);
        REQUIRE_FALSE(freed_at_close);
        REQUIRE(plexus::testing::watched_freed());
    }
}
