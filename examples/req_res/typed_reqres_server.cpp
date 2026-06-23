// Typed request/response — the server half. Serves a typed "divide" procedure:
// it returns the quotient, or an error for a zero divisor so the error leg is visible
// at the client. The in-file codecs carry the request/response wire shapes. Run
// alongside typed_reqres_client.

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/procedure.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/mdnspp/mdnspp_discovery.h"

#include <asio/io_context.hpp>

#include <span>
#include <vector>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
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
    asio::io_context                 io;
    plexus::asio::asio_transport     transport{io};
    plexus::mdnspp::mdnspp_discovery disc{io, "_plexus._tcp.local."};

    plexus::node_options opts;
    opts.name      = "divide-server";
    opts.reconnect = plexus::io::reconnect_config{
            std::chrono::milliseconds(200), std::chrono::seconds(5), std::nullopt, std::nullopt};
    opts.redial_seed = 0xD17DE;

    plexus::node<plexus::asio::asio_policy, plexus::asio::asio_transport> node{
            io, disc, "divide-server", transport, opts};
    node.listen({"tcp", "127.0.0.1:5576"});

    using divide_procedure = plexus::procedure<div_response(div_request), pair_codec>;
    divide_procedure divide{
            node, "divide",
            [](const div_request &req) -> plexus::expected<div_response, std::error_code>
            {
                if(req.denominator == 0)
                    return plexus::expected<div_response, std::error_code>{
                            plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
                return div_response{req.numerator / req.denominator};
            }};

    io.run();
}
