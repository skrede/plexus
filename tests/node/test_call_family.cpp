// The call family over the node facade, two nodes on a shared inproc bus +
// static_discovery. It proves:
//   - a call round-trips byte-identical request/response through expected.value().bytes;
//   - the reply_info attribution is populated per the documented contract (the resolved
//     provider node_id reaches provider_identity; reception_timestamp is stamped);
//   - a second LOCAL serve on one fqn throws std::logic_error and leaves the first
//     handler serving (no side effects from the refused ctor);
//   - after dropping the procedure, a subsequent call completes with call_errc::no_handler
//     through the per-call deadline path (bounded by the deadline override, never a sleep).

#include "plexus/node.h"
#include "plexus/caller.h"
#include "plexus/procedure.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
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
using inproc_caller = plexus::caller<>;
using inproc_procedure = plexus::procedure<>;
using reply_t = plexus::reply;

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
    opts.redial_seed = 0xCA11Eu;
    opts.dial_eagerly = eager;
    return opts;
}

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// A: the CALLER node, dials eagerly so its slot toward B completes (the call_seam
// resolves the first connected peer on the dialer's slot). B: the PROVIDER node, lazy,
// accepts A's dial as the single inbound session the round-trip rides bidirectionally.
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

TEST_CASE("call family: round-trip is byte-identical, looped", "[node][call]")
{
    constexpr int k_iterations = 5;
    net n;
    n.connect();

    // The provider echoes "reply:<param>".
    inproc_procedure proc{
        n.b, "rpc",
        [](std::span<const std::byte> param, inproc_procedure::reply_fn &reply) {
            const std::string out = "reply:" + to_string(param);
            reply(plexus::wire::rpc_status::success, as_bytes(out));
        }};
    inproc_caller call{n.a, "rpc"};
    n.drive();

    int delivered = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        const std::string req = "req-" + std::to_string(i);
        std::optional<std::string> got;
        std::optional<plexus::node_id> provider;
        bool reception_stamped = false;
        call.call(as_bytes(req),
                  [&](plexus::expected<reply_t, std::error_code> r) {
                      REQUIRE(static_cast<bool>(r));
                      got = to_string(r.value().bytes);
                      if(r.value().info.provider_identity)
                          provider = r.value().info.provider_identity->node_id();
                      reception_stamped = r.value().info.reception_timestamp != 0;
                  });
        n.drive();
        REQUIRE(got.has_value());
        REQUIRE(*got == "reply:" + req);
        REQUIRE(provider.has_value());
        REQUIRE(*provider == n.id_b);
        REQUIRE(reception_stamped);
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("call family: a second local serve on one fqn throws and leaves the first serving", "[node][call]")
{
    net n;
    n.connect();

    int first_calls = 0;
    inproc_procedure proc{
        n.b, "rpc",
        [&](std::span<const std::byte>, inproc_procedure::reply_fn &reply) {
            ++first_calls;
            reply(plexus::wire::rpc_status::success, as_bytes(std::string{"first"}));
        }};

    REQUIRE_THROWS_AS(
        (inproc_procedure{n.b, "rpc",
                          [](std::span<const std::byte>, inproc_procedure::reply_fn &reply) {
                              reply(plexus::wire::rpc_status::success, {});
                          }}),
        std::logic_error);

    // The refused ctor left no side effect: the FIRST handler still answers.
    inproc_caller call{n.a, "rpc"};
    n.drive();
    std::optional<std::string> got;
    call.call(as_bytes(std::string{"x"}),
              [&](plexus::expected<reply_t, std::error_code> r) {
                  REQUIRE(static_cast<bool>(r));
                  got = to_string(r.value().bytes);
              });
    n.drive();
    REQUIRE(got == "first");
    REQUIRE(first_calls == 1);
}

TEST_CASE("call family: dropping the procedure retires it to no_handler", "[node][call]")
{
    net n;
    n.connect();

    {
        inproc_procedure proc{
            n.b, "rpc",
            [](std::span<const std::byte>, inproc_procedure::reply_fn &reply) {
                reply(plexus::wire::rpc_status::success, as_bytes(std::string{"served"}));
            }};
        n.drive();

        inproc_caller call{n.a, "rpc"};
        std::optional<std::string> got;
        call.call(as_bytes(std::string{"q"}),
                  [&](plexus::expected<reply_t, std::error_code> r) {
                      REQUIRE(static_cast<bool>(r));
                      got = to_string(r.value().bytes);
                  });
        n.drive();
        REQUIRE(got == "served");
    }
    // proc dropped here — its handler is retired.

    inproc_caller call{n.a, "rpc"};
    std::optional<std::error_code> err;
    plexus::call_options opts;
    opts.deadline = std::chrono::milliseconds(50);
    call.call(as_bytes(std::string{"q"}), opts,
              [&](plexus::expected<reply_t, std::error_code> r) {
                  REQUIRE_FALSE(static_cast<bool>(r));
                  err = r.error();
              });
    n.drive();
    REQUIRE(err.has_value());
    REQUIRE(*err == plexus::call_errc::no_handler);
}

#ifdef PLEXUS_HAS_FAMILY_SPELLING
// The codec-family spelling positive assertions. A codec slot takes a class-template codec
// FAMILY; the typed endpoint applies Family<Req> / Family<Res> over a Res(Req) signature. The
// symmetric form (one family) defaults the response family to the request family; the
// asymmetric form spells two families, request-first. echo_codec is a trivial value codec
// family (one u32), supplied here since this TU defines no codec of its own.
namespace family_check {

struct u32_value
{
    std::uint32_t value{};
};

template <typename T>
struct echo_codec
{
    using value_type = T;

    plexus::wire_bytes<> encode(const T &v) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        return plexus::wire_bytes<>{std::span<const std::byte>{owner->data(), owner->size()},
                                    std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, T &out) const
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
};

// The symmetric form defaults the response family to the request family: one spelled family
// names the same endpoint as the explicit two-family form over a single family.
static_assert(__is_same(plexus::procedure<u32_value(u32_value), echo_codec>,
                        plexus::procedure<u32_value(u32_value), echo_codec, echo_codec>));
static_assert(__is_same(plexus::caller<u32_value(u32_value), echo_codec>,
                        plexus::caller<u32_value(u32_value), echo_codec, echo_codec>));

// The asymmetric form binds two DIFFERENT families for request and response, request-first:
// the response half is alt_codec while the request half stays echo_codec, so the endpoint
// differs from the symmetric echo_codec spelling. A second codec family witnesses it.
template <typename T>
struct alt_codec
{
    using value_type = T;

    plexus::wire_bytes<> encode(const T &v) const { return echo_codec<T>{}.encode(v); }
    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, T &out) const
    {
        return echo_codec<T>{}.decode(b, out);
    }
};

using asymmetric_procedure =
    plexus::procedure<u32_value(u32_value), echo_codec, alt_codec>;
static_assert(__is_same(asymmetric_procedure,
                        plexus::procedure<u32_value(u32_value), echo_codec, alt_codec>));
static_assert(!__is_same(asymmetric_procedure,
                         plexus::procedure<u32_value(u32_value), echo_codec>));

// The one-off concrete-codec lift idiom: an alias template lifts a finished codec into the
// family slot (the P0522 template-template-argument path). It must bind and name the same
// endpoint as the direct family spelling.
template <typename>
using lifted_echo = echo_codec<u32_value>;
static_assert(__is_same(plexus::procedure<u32_value(u32_value), lifted_echo>,
                        plexus::procedure<u32_value(u32_value), lifted_echo, lifted_echo>));
static_assert(plexus::typed_codec<echo_codec<u32_value>>);
static_assert(plexus::typed_codec<alt_codec<u32_value>>);

}

TEST_CASE("call family: the family-form spelling names the same caller/procedure endpoint",
          "[node][call][family]")
{
    using family_check::u32_value;
    using family_procedure = plexus::procedure<u32_value(u32_value), family_check::echo_codec>;
    using family_caller    = plexus::caller<u32_value(u32_value), family_check::echo_codec>;

    net n;
    n.connect();

    family_procedure proc{
        n.b, "rpc",
        [](const u32_value &req) -> plexus::expected<u32_value, std::error_code> {
            return u32_value{req.value + 1};
        }};
    family_caller call{n.a, "rpc"};
    n.drive();

    std::optional<std::uint32_t> got;
    call.call(u32_value{41},
              [&](plexus::expected<u32_value, std::error_code> r) {
                  REQUIRE(static_cast<bool>(r));
                  got = r.value().value;
              });
    n.drive();
    REQUIRE(got == 42u);
}
#endif
