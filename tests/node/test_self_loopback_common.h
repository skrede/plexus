#ifndef HPP_GUARD_PLEXUS_TESTS_NODE_TEST_SELF_LOOPBACK_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_NODE_TEST_SELF_LOOPBACK_COMMON_H

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/typed_codec.h"
#include "plexus/wire_bytes.h"
#include "plexus/loopback_node.h"
#include "plexus/node_options.h"

#include "plexus/io/process_loopback_channel.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/discovery/static_discovery.h"

#include <span>
#include <atomic>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus_test {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_executor;
using plexus::discovery::static_discovery;

// The pure intra-node Policy: the loopback channel IS its byte_channel_type, so a single-transport
// node binds it concretely (zero erasure, the typed fast path engages). The step-executor and
// virtual-clock timer are reused from inproc (a DRY consolidation, not a new mechanism).
struct loopback_policy
{
    using executor_type     = inproc_executor<> &;
    using byte_channel_type = plexus::io::process_loopback_channel<loopback_policy>;
    using timer_type        = inproc_timer<>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<loopback_policy>);

struct sample
{
    std::uint32_t value{};
};

struct counting_codec
{
    using value_type = sample;

    std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);

    plexus::wire_bytes<> encode(const sample &v) const
    {
        ++*encodes;
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, sample &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0xABCD1234u, "sample"};
    }
};

static_assert(plexus::typed_codec<counting_codec>);

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

struct fixture
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};
    plexus::loopback_host<loopback_policy> host{ex, disc, make_id(0x70)};

    plexus::loopback_node<loopback_policy> &node()
    {
        return host.node();
    }
    void drive()
    {
        ex.drain();
    }
};

}

#endif
