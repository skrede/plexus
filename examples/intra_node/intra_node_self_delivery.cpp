// A node is a complete data-plane entry point to itself, out of the box. One node built with NO
// explicit transport delivers its own publish(topic) to its own subscribe(topic), in-process. On
// the typed lane the subscriber receives the SAME object by address with the codec's encode never
// invoked (zero-copy); a closing bytes-lane section shows the posted self-route for raw payloads.

#include "plexus/node_id.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"
#include "plexus/loopback_node.h"

#include "plexus/io/message_info.h"
#include "plexus/io/process_loopback_channel.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <atomic>
#include <memory>
#include <vector>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <iostream>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_executor;
using plexus::discovery::static_discovery;

// The pure intra-node Policy: the loopback channel IS its byte_channel_type, so the single-transport
// node binds it concretely (zero erasure, the typed fast path engages). The step-executor and
// virtual-clock timer are the reusable inproc host substrate.
struct intra_node_policy
{
    using executor_type     = inproc_executor<> &;
    using byte_channel_type = plexus::io::process_loopback_channel<intra_node_policy>;
    using timer_type        = inproc_timer<>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<intra_node_policy>);

struct reading
{
    std::uint32_t value{};
};

struct reading_codec
{
    using value_type = reading;

    std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);

    plexus::wire_bytes<> encode(const reading &r) const
    {
        ++*encodes;
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((r.value >> (8 * i)) & 0xff);
        const std::span<const std::byte> view{owner->data(), owner->size()};
        return {view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, reading &out) const
    {
        if(b.size() != 4)
            return plexus::expected<void, std::error_code>{
                plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(b[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const { return {0x5E4501u, "reading"}; }
};

static_assert(plexus::typed_codec<reading_codec>);

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// One node owning its intra-node leaf, with no caller transport: the loopback_host stands up the
// self-delivering node from the inproc substrate and a seedless static discovery.
struct self_node
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};
    plexus::loopback_host<intra_node_policy> host{ex, disc, make_id(0x70)};

    plexus::loopback_node<intra_node_policy> &node() { return host.node(); }
    void drain() { ex.drain(); }
};

void typed_fast_path()
{
    std::cout << "== typed lane: a node delivers its own publish to itself, zero-copy ==\n";
    self_node n;
    reading_codec codec;
    auto encodes = codec.encodes;

    const reading *received = nullptr;
    plexus::subscriber<reading_codec> sub{
        n.node(), "telemetry", [&](const reading &r, const plexus::io::message_info &info)
        {
            received = &r;
            std::cout << "  delivered value=" << r.value
                      << "  from_intra_process=" << std::boolalpha << info.from_intra_process << '\n';
        }};
    plexus::publisher<reading_codec> pub{n.node(), "telemetry", plexus::typed_publisher_options{}, codec};
    n.drain();

    auto loan                = pub.borrow();
    loan->value              = 42;
    const reading *published = &*loan;
    std::cout << "  published object @ " << static_cast<const void *>(published) << '\n';
    pub.publish(std::move(loan));
    n.drain();

    std::cout << "  same-address witness = " << std::boolalpha << (received == published) << '\n';
    std::cout << "  encodes = " << encodes->load() << "  (zero == no serialization)\n";
}

void bytes_lane()
{
    std::cout << "== bytes lane: the same self-route delivers raw payloads, posted ==\n";
    self_node n;

    plexus::subscriber<> sub{n.node(), "telemetry", [&](std::span<const std::byte> b)
                             { std::cout << "  bytes sub received " << b.size() << " bytes\n"; }};
    plexus::publisher<> pub{n.node(), "telemetry"};
    n.drain();

    const std::array<std::byte, 3> payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
    pub.publish(payload);
    n.drain();
}

int main()
{
    typed_fast_path();
    bytes_lane();
}
