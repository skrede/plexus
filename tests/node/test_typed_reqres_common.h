// The typed request/response family over the node facade: a typed caller round-trips a
// typed request/response with a typed procedure through real encode/decode, provider/decode
// failure legs, a bytes-procedure interop fallback, and the second-local-serve refusal. The
// codec is a hand-rolled trivial struct codec defined here — plexus never names a serializer
// library.
#pragma once

#include "plexus/node.h"
#include "plexus/caller.h"
#include "plexus/expected.h"
#include "plexus/procedure.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <system_error>

namespace typed_reqres_fixture {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

// A request: one 32-bit number. A response: one 32-bit number. The codecs serialize a
// u32 little-endian into a fresh wire_bytes, and decode strictly (exactly 4 bytes).
struct request_t
{
    std::uint32_t value{};
};

struct response_t
{
    std::uint32_t value{};
};

template<typename T>
struct u32_codec
{
    using value_type = T;

    plexus::wire_bytes<> encode(const T &v) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, T &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }
};

using req_codec = u32_codec<request_t>;
using res_codec = u32_codec<response_t>;

using typed_caller    = plexus::caller<response_t(request_t), u32_codec>;
using typed_procedure = plexus::procedure<response_t(request_t), u32_codec>;
using bytes_procedure = plexus::procedure<>;

static_assert(plexus::typed_codec<req_codec>);
static_assert(plexus::typed_codec<res_codec>);

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
    opts.redial_seed  = 0x7ED50u;
    opts.dial_eagerly = eager;
    return opts;
}

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

struct net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery disc{{}};

    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};

    inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};
    inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};

    void drive()
    {
        ex.drain();
    }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }
};

} // namespace typed_reqres_fixture
