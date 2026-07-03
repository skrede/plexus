// The visible zero-serialization fast path, single process. Two nodes share one
// in-process bus and executor. A typed publisher borrows an object, mutates it, and
// publishes it; the typed subscriber receives the SAME object by address with the
// codec's encode never invoked (from_intra_process true). A second section adds a
// bytes subscriber on the same topic: now the publish encodes once and BOTH the typed
// (by address) and bytes (decoded) subscribers are delivered — the graceful fallback.

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"

#include "plexus/io/message_info.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <span>
#include <vector>
#include <chrono>
#include <memory>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <system_error>

using plexus::inproc::inproc_policy;
using transport_t = plexus::inproc::inproc_transport<>;
using node_t      = plexus::node<inproc_policy, transport_t>;

struct reading
{
    std::uint32_t sensor{};
    std::uint32_t value{};
};

// The encode counter is shared so the test body sees a publisher's encode activity
// regardless of which codec copy the endpoint holds.
struct reading_codec
{
    using value_type                          = reading;
    std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);

    plexus::wire_bytes<> encode(const reading &r) const
    {
        ++*encodes;
        auto owner = std::make_shared<std::vector<std::byte>>(8);
        for(int i = 0; i < 4; ++i)
        {
            (*owner)[i]     = static_cast<std::byte>((r.sensor >> (8 * i)) & 0xff);
            (*owner)[4 + i] = static_cast<std::byte>((r.value >> (8 * i)) & 0xff);
        }
        const std::span<const std::byte> view{owner->data(), owner->size()};
        return {view, std::move(owner)};
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

plexus::node_options opts(std::uint64_t seed, bool eager)
{
    plexus::node_options o;
    o.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                  std::chrono::milliseconds(2000), std::nullopt,
                                                  std::nullopt};
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
// subscriber node is the single eager dialer (the canonical inproc topology).
struct net
{
    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex{bus};
    transport_t ta{ex, bus};
    transport_t tb{ex, bus};
    plexus::discovery::static_discovery disc{{}};

    node_t pub_node{ex, disc, id_of(0x0A), ta, opts(0xA, /*eager=*/false)};
    node_t sub_node{ex, disc, id_of(0x0B), tb, opts(0xB, /*eager=*/true)};

    net()
    {
        sub_node.listen({"inproc", "host-b:6000"});
        pub_node.listen({"inproc", "host-a:5000"});
        ex.drain();
    }
};

// The fast path: a typed in-process subscriber receives the publisher's borrowed
// object BY ADDRESS, the codec's encode never invoked.
void fast_path()
{
    std::cout << "== fast path: in-process typed subscriber ==\n";
    net n;
    reading_codec codec;
    auto encodes = codec.encodes;

    const reading *received = nullptr;
    plexus::subscriber<reading_codec> typed_sub{
        n.sub_node, "telemetry", [&](const reading &r, const plexus::io::message_info &info)
        {
            received = &r;
            std::cout << "  delivered value=" << r.value
                      << "  from_intra_process=" << std::boolalpha << info.from_intra_process
                      << '\n';
        }};
    plexus::publisher<reading_codec> pub{n.pub_node, "telemetry", plexus::typed_publisher_options{},
                                         codec};
    n.ex.drain();

    auto loan                = pub.borrow();
    loan->value              = 42;
    const reading *published = &*loan;
    std::cout << "  published object @ " << static_cast<const void *>(published) << '\n';
    pub.publish(std::move(loan));
    n.ex.drain();

    std::cout << "  same-address witness = " << std::boolalpha << (received == published) << '\n';
    std::cout << "  encodes = " << encodes->load() << "  (zero == no serialization)\n";
}

// The graceful fallback: a bytes subscriber cannot take the object lane, so the same
// typed publisher serializes exactly once and the wire frame is delivered.
void fallback()
{
    std::cout << "== fallback: a bytes-only subscriber ==\n";
    net n;
    reading_codec codec;
    auto encodes = codec.encodes;

    plexus::subscriber<> bytes_sub{n.sub_node, "telemetry", [&](std::span<const std::byte> b)
                                   {
                                       std::cout << "  bytes sub received " << b.size()
                                                 << " bytes (the consumer decodes)\n";
                                   }};
    plexus::publisher<reading_codec> pub{n.pub_node, "telemetry", plexus::typed_publisher_options{},
                                         codec};
    n.ex.drain();

    pub.publish(reading{1, 99});
    n.ex.drain();

    std::cout << "  encodes = " << encodes->load() << "  (one == the bytes leg serialized)\n";
}

int main()
{
    fast_path();
    fallback();
}
