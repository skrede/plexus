#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_RELAY_LINK_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_RELAY_LINK_H

#include "dtls_relay.h"
#include "dtls_identity_fixture.h"

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_transport.h"

#include <asio/io_context.hpp>

#include <memory>
#include <string>
#include <utility>

namespace plexus::dtls_test {

// A dialed link where the client dials a programmable lossy relay that forwards to
// the real server, so the handshake flights can be dropped/reordered/duplicated.
// The cred pair is injected so the verify cases can mis-pin either side.
struct relay_link
{
    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    ptls::dtls_transport server;
    ptls::dtls_transport client;
    std::unique_ptr<relay> link;

    std::unique_ptr<ptls::dtls_channel> accepted;
    std::unique_ptr<ptls::dtls_channel> dialed;
    bool dial_failed{false};

    relay_link(ptls::tls_credential srv, ptls::tls_credential cli)
            : server_cred(std::move(srv))
            , client_cred(std::move(cli))
            , server(io, server_cred)
            , client(io, client_cred)
    {
        server.on_accepted([this](std::unique_ptr<ptls::dtls_channel> ch) { accepted = std::move(ch); });
        client.on_dialed([this](std::unique_ptr<ptls::dtls_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.on_dial_failed([this](const pio::endpoint &, pio::io_error) { dial_failed = true; });

        server.listen({"dtls", "127.0.0.1:0"});
        link = std::make_unique<relay>(io, server.port());
        client.dial({"dtls", "127.0.0.1:" + std::to_string(link->port())});
    }
};

}

#endif
