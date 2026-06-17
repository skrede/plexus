// Project a live typed topic to CSV on stdout, single process. A node.log<Codec,
// Projection> handle decodes one topic's samples and writes a columnar record per sample
// (the analysis-friendly format for plotting); a projection names the columns and emits
// the field values. A type with no projection would fall back to the operator<< text
// floor. The handle is an RAII subscriber sibling: it receives the in-process object
// fast-path with no encode, so the output is identical whether the publish took the loan
// lane or the bytes lane. Public API only; self-terminating (no backends, no mDNS).

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/value_logger.h"
#include "plexus/value_projection.h"
#include "plexus/value_logger_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <span>
#include <array>
#include <vector>
#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string_view>
#include <system_error>

using plexus::inproc::inproc_policy;
using transport_t = plexus::inproc::inproc_transport<>;
using node_t      = plexus::node<inproc_policy, transport_t>;

struct reading
{
    std::uint32_t sensor{};
    std::uint32_t value{};
};

// The codec decodes bytes -> reading (and encodes for the bytes lane / a remote peer).
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
        auto u32 = [&](int o) {
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

// The projection names the CSV columns for reading and emits one record's field values into
// a caller-owned buffer (the logger reuses one buffer; the projection appends, never mints a
// fresh string per record). A type without a projection logs through the operator<< floor.
struct reading_projection
{
    std::array<std::string_view, 2> columns() const { return {"sensor", "value"}; }

    void emit_fields(const reading &r, std::string &row, char delim) const
    {
        row += std::to_string(r.sensor);
        row += delim;
        row += std::to_string(r.value);
    }

    void emit_json(const reading &r, std::string &obj) const
    {
        obj += "\"sensor\":";
        obj += std::to_string(r.sensor);
        obj += ",\"value\":";
        obj += std::to_string(r.value);
    }
};

plexus::node_options opts(std::uint64_t seed, bool eager)
{
    plexus::node_options o;
    o.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                               std::chrono::milliseconds(2000),
                                               std::nullopt, std::nullopt};
    o.redial_seed  = seed;
    o.dial_eagerly = eager;
    return o;
}

plexus::node_id id_of(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// Two nodes on one in-process bus and executor: the publisher node stays lazy, the
// logging node is the single eager dialer (the canonical inproc topology).
struct net
{
    plexus::inproc::inproc_bus<>        bus;
    plexus::inproc::inproc_executor<>   ex{bus};
    transport_t                         ta{ex, bus};
    transport_t                         tb{ex, bus};
    plexus::discovery::static_discovery disc{{}};

    node_t pub_node{ex, disc, id_of(0x0A), ta, opts(0xA, /*eager=*/false)};
    node_t log_node{ex, disc, id_of(0x0B), tb, opts(0xB, /*eager=*/true)};

    net()
    {
        log_node.listen({"inproc", "host-b:6000"});
        pub_node.listen({"inproc", "host-a:5000"});
        ex.drain();
    }
};

int main()
{
    net n;

    // The logging handle: decode the "telemetry" topic with reading_codec, project each
    // value to CSV columns on std::cout. The ostream MUST outlive the handle. The handle is
    // RAII and move-only — keep it alive for the session, drop it to retire.
    auto logger = n.log_node.log<reading_codec, reading_projection>(
        "telemetry",
        plexus::value_logger_options{.out = std::cout, .format = plexus::log_format::csv},
        reading_codec{}, reading_projection{});

    plexus::publisher<reading_codec> pub{
        n.pub_node, "telemetry", plexus::typed_publisher_options{}, reading_codec{}};
    n.ex.drain();

    // Publish a few samples; each is decoded + projected to a CSV row on stdout. The header
    // row (publication_sequence + the projection columns) is written once on the first row.
    for(std::uint32_t i = 0; i < 5; ++i)
    {
        pub.publish(reading{/*sensor=*/7, /*value=*/100 + i});
        n.ex.drain();
    }

    return 0;
}
