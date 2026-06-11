// Bytes request/response — the client half. Discovers the server, calls "uppercase"
// with a line of bytes, prints the reply or the error code, then stops. Run alongside
// bytes_reqres_server.

#include "plexus/node.h"
#include "plexus/caller.h"
#include "plexus/expected.h"
#include "plexus/call_error.h"
#include "plexus/node_options.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/mdnspp/mdnspp_discovery.h"

#include <asio/steady_timer.hpp>
#include <asio/io_context.hpp>

#include <span>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <functional>

int main()
{
    asio::io_context io;
    plexus::asio::asio_transport transport{io};
    plexus::mdnspp::mdnspp_discovery disc{io, "_plexus._tcp.local."};

    plexus::node_options opts;
    opts.name = "uppercase-client";
    opts.dial_eagerly = true;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(200),
                                                  std::chrono::seconds(5),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed = 0xC11E7;

    plexus::node<plexus::asio::asio_policy, plexus::asio::asio_transport> node{
        io, disc, "uppercase-client", transport, opts};
    node.listen({"tcp", "127.0.0.1:5573"});

    plexus::caller<plexus::asio::asio_policy> uppercase{node, "uppercase"};

    // Retry once a second until a provider is discovered and answers (no_provider while
    // mDNS converges is expected, so re-arm; any reply or transformed error is terminal).
    const std::string request = "hello, plexus";
    asio::steady_timer retry{io};
    std::function<void()> attempt = [&] {
        uppercase.call(
            std::span<const std::byte>{
                reinterpret_cast<const std::byte *>(request.data()), request.size()},
            [&](plexus::expected<plexus::reply, std::error_code> r) {
                if(r)
                {
                    std::cout << "reply: "
                              << std::string{reinterpret_cast<const char *>(r->bytes.data()),
                                             r->bytes.size()} << '\n';
                    io.stop();
                }
                else if(r.error() == plexus::call_errc::no_provider)
                {
                    retry.expires_after(std::chrono::seconds(1));
                    retry.async_wait([&](auto) { attempt(); });
                }
                else
                {
                    std::cout << "error: " << r.error().message() << '\n';
                    io.stop();
                }
            });
    };
    attempt();

    io.run();
}
