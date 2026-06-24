#include "dtls_test_support.h"

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_transport.h"

#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>

namespace pdt  = plexus::dtls_test;
namespace ptls = plexus::tls;
namespace pio  = plexus::io;

namespace {

// A dialed loopback DTLS link via the transport, constructed with a LOW per-message
// ceiling (global_default) so the proof is whether the transport threads that ceiling
// to the channels it mints. listen("dtls","127.0.0.1:0") then dial the bound port; the
// accepted channel lands via on_accepted, the dialed channel via on_dialed.
struct ceiling_link
{
    ::asio::io_context   io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    ptls::dtls_transport server;
    ptls::dtls_transport client;

    std::unique_ptr<ptls::dtls_channel> accepted;
    std::unique_ptr<ptls::dtls_channel> dialed;
    bool                                dial_failed{false};
    std::vector<std::vector<std::byte>> server_received;
    std::optional<pio::io_error>        dialed_error;

    ceiling_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id, std::size_t max_payload, std::size_t global_default, std::size_t reassembly_budget)
            : server_cred(pdt::pin_one(server_id, client_id.digest))
            , client_cred(pdt::pin_one(client_id, server_id.digest))
            , server(io, server_cred, max_payload, global_default, reassembly_budget)
            , client(io, client_cred, max_payload, global_default, reassembly_budget)
    {
        server.on_accepted(
                [this](std::unique_ptr<ptls::dtls_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([this](std::span<const std::byte> d) { server_received.emplace_back(d.begin(), d.end()); });
                });
        client.on_dialed(
                [this](std::unique_ptr<ptls::dtls_channel> ch, const pdt::pio::endpoint &)
                {
                    dialed = std::move(ch);
                    dialed->on_error([this](pio::io_error e) { dialed_error = e; });
                });
        client.on_dial_failed([this](const pdt::pio::endpoint &, pdt::pio::io_error) { dial_failed = true; });

        server.listen({"dtls", "127.0.0.1:0"});
        client.dial({"dtls", "127.0.0.1:" + std::to_string(server.port())});
    }

    template<typename Pred>
    void pump_until(Pred pred, std::chrono::milliseconds timeout = std::chrono::milliseconds{6000})
    {
        pdt::pump_until(io, pred, timeout);
    }
};

}

TEST_CASE("dtls.transport_ceiling: a transport-configured LOW per-message ceiling reaches the "
          "minted channel and rejects an oversize send, looped",
          "[dtls][envelope16]")
{
    // The fix proof: dtls_transport now threads its global_default per-MESSAGE ceiling (and the
    // reassembly_budget) to every dtls_channel it mints at dial + accept. Construct the pair with
    // a LOW 4096-byte ceiling: complete a live handshake, then send a 5000-byte message. The send
    // is REJECTED with message_too_large ONLY IF the low ceiling reached the minted dialed channel
    // — at the old hardcoded 8 MiB default a 5000-byte message would deliver fine. The reject
    // direction is deterministic (no best-effort loss dependence), so it is the threading proof.
    pdt::identity_fixture srv("ceil_srv");
    pdt::identity_fixture cli("ceil_cli");

    constexpr std::size_t k_ceiling = 4096; // far below the 8 MiB hardcoded default
    constexpr std::size_t k_budget  = 16u * 1024u * 1024u;
    constexpr std::size_t k_over    = 5000; // > k_ceiling, << the 8 MiB default

    constexpr int k_iterations = 50;
    int           proven       = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        ceiling_link l(srv, cli, ptls::dtls_channel::default_max_payload, k_ceiling, k_budget);
        l.pump_until([&] { return (l.accepted && l.dialed) || l.dial_failed; });
        REQUIRE_FALSE(l.dial_failed);
        REQUIRE(l.accepted != nullptr);
        REQUIRE(l.dialed != nullptr);

        l.dialed_error.reset();
        std::vector<std::byte> frame(k_over, std::byte{0xAB});
        l.dialed->send(std::span<const std::byte>{frame});
        l.pump_until([&] { return l.dialed_error.has_value(); }, std::chrono::milliseconds{500});

        REQUIRE(l.dialed_error == pio::io_error::message_too_large); // the LOW ceiling threaded through
        REQUIRE(l.server_received.empty());                          // the oversize message never delivered
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
