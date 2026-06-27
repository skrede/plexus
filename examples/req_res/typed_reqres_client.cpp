// Typed request/response — the client half. Discovers the server, issues a valid
// divide and a divide-by-zero, and prints the typed result or the error. The in-file
// codecs are identical to the server's. Run alongside typed_reqres_server.

#include "plexus/node.h"
#include "plexus/caller.h"
#include "plexus/expected.h"
#include "plexus/call_error.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"

#include "plexus/discovery/multicast_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/udp_multicast_socket.h"

#include <asio/steady_timer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/address_v4.hpp>

#include <span>
#include <vector>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <optional>
#include <functional>
#include <system_error>

struct div_request
{
    std::int32_t numerator{};
    std::int32_t denominator{};
};
struct div_response
{
    std::int32_t quotient{};
};

template<typename T>
struct pair_codec
{
    using value_type = T;

    plexus::wire_bytes<> encode(const T &v) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(sizeof(T));
        std::memcpy(owner->data(), &v, sizeof(T));
        return {std::span<const std::byte>{owner->data(), owner->size()}, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, T &out) const
    {
        if(b.size() != sizeof(T))
            return plexus::expected<void, std::error_code>{
                plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::memcpy(&out, b.data(), sizeof(T));
        return {};
    }
};

int main()
{
    asio::io_context io;
    plexus::asio::asio_transport transport{io};
    plexus::asio::udp_multicast_socket mc_socket{io, asio::ip::make_address_v4("239.255.0.7"), 7447, 4};
    plexus::discovery::multicast_discovery<plexus::asio::udp_multicast_socket, plexus::asio::asio_policy>
        disc{io, mc_socket};

    plexus::node_options opts;
    opts.name         = "divide-client";
    opts.dial_eagerly = true;
    opts.reconnect    = plexus::io::reconnect_config{
        std::chrono::milliseconds(200), std::chrono::seconds(5), std::nullopt, std::nullopt};
    opts.redial_seed = 0xC11D1;

    plexus::node<plexus::asio::asio_policy, plexus::asio::asio_transport> node{
        io, disc, "divide-client", transport, opts};
    node.listen({"tcp", "127.0.0.1:5577"});

    using divide_caller = plexus::caller<div_response(div_request), pair_codec>;
    divide_caller divide{node, "divide"};

    auto print = [](const char *label, plexus::expected<div_response, std::error_code> r)
    {
        if(r)
            std::cout << label << ": quotient " << r->quotient << '\n';
        else
            std::cout << label << ": error \"" << r.error().message() << "\"\n";
    };

    // The divide-by-zero leg fires once the provider is up (its error reply is the
    // visible error path).
    auto by_zero = [&]
    {
        divide.call(div_request{1, 0},
                    [&](plexus::expected<div_response, std::error_code> r)
                    {
                        print("1 / 0", std::move(r));
                        io.stop();
                    });
    };

    // Retry the valid divide until a provider answers (no_provider while discovery converges
    // re-arms); on the first real answer, the session is up, so fire the error leg too.
    asio::steady_timer retry{io};
    std::function<void()> attempt = [&]
    {
        divide.call(div_request{42, 7},
                    [&](plexus::expected<div_response, std::error_code> r)
                    {
                        if(!r && r.error() == plexus::call_errc::no_provider)
                        {
                            retry.expires_after(std::chrono::seconds(1));
                            retry.async_wait([&](auto)
                                             { attempt(); });
                            return;
                        }
                        print("42 / 7", std::move(r));
                        by_zero();
                    });
    };
    attempt();

    io.run();
}
