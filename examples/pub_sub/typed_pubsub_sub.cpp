// Typed pub/sub — the subscriber half. Discovers the publisher over mDNS and prints
// each decoded reading that arrives on "telemetry". The in-file codec is identical to
// the publisher's. Run alongside typed_pubsub_pub.

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/subscriber.h"
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
#include <iostream>
#include <optional>
#include <system_error>

struct reading
{
    std::uint32_t sensor{};
    std::uint32_t value{};
};

struct reading_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &r) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(8);
        for(int i = 0; i < 4; ++i)
        {
            (*owner)[i]     = static_cast<std::byte>((r.sensor >> (8 * i)) & 0xff);
            (*owner)[4 + i] = static_cast<std::byte>((r.value >> (8 * i)) & 0xff);
        }
        return {std::span<const std::byte>{owner->data(), owner->size()}, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, reading &out) const
    {
        if(b.size() != 8)
            return plexus::expected<void, std::error_code>{
                plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        auto u32 = [&](int o)
        {
            std::uint32_t v = 0;
            for(int i = 0; i < 4; ++i)
                v |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(b[o + i])) << (8 * i);
            return v;
        };
        out = {u32(0), u32(4)};
        return {};
    }

    plexus::type_identity type_info() const { return {0x5E4501u, "reading"}; }
};

int main()
{
    asio::io_context io;
    plexus::asio::asio_transport transport{io};
    plexus::mdnspp::mdnspp_discovery disc{io, "_plexus._tcp.local."};

    plexus::node_options opts;
    opts.name         = "telemetry-subscriber";
    opts.dial_eagerly = true;
    opts.reconnect    = plexus::io::reconnect_config{
        std::chrono::milliseconds(200), std::chrono::seconds(5), std::nullopt, std::nullopt};
    opts.redial_seed = 0x5BB5C;

    plexus::node<plexus::asio::asio_policy, plexus::asio::asio_transport> node{
        io, disc, "telemetry-subscriber", transport, opts};
    node.listen({"tcp", "127.0.0.1:5575"});

    plexus::subscriber<reading_codec> topic{
        node, "telemetry", [](const reading &r)
        { std::cout << "reading: sensor=" << r.sensor << " value=" << r.value << '\n'; }};

    io.run();
}
