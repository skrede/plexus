// The typed request/response family over the node facade: a typed caller round-trips a
// typed request/response with a typed procedure through real encode/decode, a provider
// handler error rides back to the caller with its VALUE preserved under
// provider_category, a provider request-decode failure surfaces as deserialize_failed, a
// reply-decode failure surfaces as deserialize_failed, a typed caller against a BYTES
// procedure replying a bare error falls back to call_errc::error (interop), and a second
// LOCAL serve on one fqn still throws on the typed form. The codec is a hand-rolled
// trivial struct codec defined here — plexus never names a serializer library.

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

namespace {

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

template <typename T>
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
            return plexus::expected<void, std::error_code>{
                plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }
};

using req_codec = u32_codec<request_t>;
using res_codec = u32_codec<response_t>;

using typed_caller = plexus::caller<response_t(request_t), req_codec, res_codec>;
using typed_procedure =
    plexus::procedure<response_t(request_t), req_codec, res_codec>;
using bytes_procedure = plexus::procedure<>;

static_assert(plexus::typed_codec<req_codec>);
static_assert(plexus::typed_codec<res_codec>);

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                  std::chrono::milliseconds(2000),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed = 0x7ED50u;
    opts.dial_eagerly = eager;
    return opts;
}

std::span<const std::byte> as_bytes(const std::string &s)
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

    void drive() { ex.drain(); }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }
};

}

TEST_CASE("typed reqres: round-trip decodes the handler's response value", "[node][typed][call]")
{
    net n;
    n.connect();

    typed_procedure proc{
        n.b, "rpc",
        [](const request_t &req) -> plexus::expected<response_t, std::error_code> {
            return response_t{req.value * 2};
        }};
    typed_caller call{n.a, "rpc"};
    n.drive();

    std::optional<std::uint32_t> got;
    call.call(request_t{21},
              [&](plexus::expected<response_t, std::error_code> r) {
                  REQUIRE(static_cast<bool>(r));
                  got = r.value().value;
              });
    n.drive();
    REQUIRE(got.has_value());
    REQUIRE(*got == 42);
}

TEST_CASE("typed reqres: a provider handler error preserves its value under provider_category",
          "[node][typed][call]")
{
    net n;
    n.connect();

    constexpr int k_provider_value = 77;
    typed_procedure proc{
        n.b, "rpc",
        [](const request_t &) -> plexus::expected<response_t, std::error_code> {
            return plexus::expected<response_t, std::error_code>{
                plexus::unexpect,
                std::error_code{k_provider_value, plexus::call_category()}};
        }};
    typed_caller call{n.a, "rpc"};
    n.drive();

    std::optional<std::error_code> err;
    call.call(request_t{1},
              [&](plexus::expected<response_t, std::error_code> r) {
                  REQUIRE_FALSE(static_cast<bool>(r));
                  err = r.error();
              });
    n.drive();
    REQUIRE(err.has_value());
    REQUIRE(err->value() == k_provider_value);
    REQUIRE(&err->category() == &plexus::provider_category());
}

TEST_CASE("typed reqres: a provider request-decode failure surfaces as deserialize_failed",
          "[node][typed][call]")
{
    net n;
    n.connect();

    bool handler_ran = false;
    typed_procedure proc{
        n.b, "rpc",
        [&](const request_t &) -> plexus::expected<response_t, std::error_code> {
            handler_ran = true;
            return response_t{0};
        }};
    // A BYTES caller sends a malformed (non-4-byte) request to the typed procedure.
    plexus::caller<> raw{n.a, "rpc"};
    n.drive();

    std::optional<std::error_code> err;
    raw.call(as_bytes(std::string{"xyz"}),
             [&](plexus::expected<plexus::reply, std::error_code> r) {
                 REQUIRE_FALSE(static_cast<bool>(r));
                 err = r.error();
             });
    n.drive();
    REQUIRE(err.has_value());
    REQUIRE(*err == plexus::call_errc::deserialize_failed);
    REQUIRE_FALSE(handler_ran);
}

TEST_CASE("typed reqres: a typed caller against a bytes procedure replying error falls back to error",
          "[node][typed][call]")
{
    net n;
    n.connect();

    // A BYTES procedure replies a bare rpc_status::error with no varint payload.
    bytes_procedure proc{
        n.b, "rpc",
        [](std::span<const std::byte>, bytes_procedure::reply_fn &reply) {
            reply(plexus::wire::rpc_status::error, {});
        }};
    typed_caller call{n.a, "rpc"};
    n.drive();

    std::optional<std::error_code> err;
    call.call(request_t{5},
              [&](plexus::expected<response_t, std::error_code> r) {
                  REQUIRE_FALSE(static_cast<bool>(r));
                  err = r.error();
              });
    n.drive();
    REQUIRE(err.has_value());
    REQUIRE(*err == plexus::call_errc::error);
}

TEST_CASE("typed reqres: a reply-decode failure surfaces as deserialize_failed", "[node][typed][call]")
{
    net n;
    n.connect();

    // A BYTES procedure replies success with a malformed (non-4-byte) response payload.
    bytes_procedure proc{
        n.b, "rpc",
        [](std::span<const std::byte>, bytes_procedure::reply_fn &reply) {
            reply(plexus::wire::rpc_status::success, as_bytes(std::string{"no"}));
        }};
    typed_caller call{n.a, "rpc"};
    n.drive();

    std::optional<std::error_code> err;
    call.call(request_t{9},
              [&](plexus::expected<response_t, std::error_code> r) {
                  REQUIRE_FALSE(static_cast<bool>(r));
                  err = r.error();
              });
    n.drive();
    REQUIRE(err.has_value());
    REQUIRE(*err == plexus::call_errc::deserialize_failed);
}

TEST_CASE("typed reqres: a second local serve on one fqn throws on the typed form", "[node][typed][call]")
{
    net n;
    n.connect();

    typed_procedure proc{
        n.b, "rpc",
        [](const request_t &) -> plexus::expected<response_t, std::error_code> {
            return response_t{0};
        }};

    REQUIRE_THROWS_AS(
        (typed_procedure{n.b, "rpc",
                         [](const request_t &) -> plexus::expected<response_t, std::error_code> {
                             return response_t{1};
                         }}),
        std::logic_error);
}

#ifdef PLEXUS_HAS_FAMILY_SPELLING
// The codec-family spelling assertions. rpc_procedure<Sig, Family> / rpc_caller<Sig, Family>
// expand one class-template codec family to the per-half codecs Family<Req> / Family<Res> over
// a Res(Req) signature. u32_codec is exactly such a family. The aliases must name the SAME types
// as the per-half four-parameter form (the static_asserts) and round-trip identically.
static_assert(__is_same(plexus::rpc_procedure<response_t(request_t), u32_codec>,
                        plexus::procedure<response_t(request_t), req_codec, res_codec>));
static_assert(__is_same(plexus::rpc_caller<response_t(request_t), u32_codec>,
                        plexus::caller<response_t(request_t), req_codec, res_codec>));

TEST_CASE("typed reqres: the family-form spelling round-trips the typed reqres", "[node][typed][call][family]")
{
    using family_procedure = plexus::rpc_procedure<response_t(request_t), u32_codec>;
    using family_caller    = plexus::rpc_caller<response_t(request_t), u32_codec>;

    net n;
    n.connect();

    family_procedure proc{
        n.b, "rpc",
        [](const request_t &req) -> plexus::expected<response_t, std::error_code> {
            return response_t{req.value * 2};
        }};
    family_caller call{n.a, "rpc"};
    n.drive();

    std::optional<std::uint32_t> got;
    call.call(request_t{21},
              [&](plexus::expected<response_t, std::error_code> r) {
                  REQUIRE(static_cast<bool>(r));
                  got = r.value().value;
              });
    n.drive();
    REQUIRE(got == 42u);
}
#endif
