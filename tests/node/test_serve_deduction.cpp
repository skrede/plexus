// The negative-compile probe for the deduced serve form, re-pointed at the real
// detail::deducible_handler. node.serve<Family>(name, handler) deduces Res(Req) from a
// handler with a SINGLE concrete call signature; a generic lambda ([](auto){}) or an
// overloaded-operator() struct has no single signature to deduce, so the factory rejects it
// with a "spell Sig explicitly" diagnostic rather than silently mis-deduce. This TU asserts
// the concept fires the rejection ONLY for genuinely non-deducible callables, that a
// concrete handler is accepted, and that the deduced serve/caller pair round-trips a typed
// request/response over the node facade.

#include "plexus/node.h"
#include "plexus/caller.h"
#include "plexus/expected.h"
#include "plexus/procedure.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"

#include "plexus/detail/function_traits.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <system_error>

namespace {

using plexus::detail::deducible_handler;

struct request_t
{
    std::uint32_t value{};
};

struct response_t
{
    std::uint32_t value{};
};

// The positive case: a concrete, non-generic handler with a single deducible signature.
using concrete_handler =
        decltype([](const request_t &) -> plexus::expected<response_t, std::error_code>
                 { return response_t{0}; });

// Negative case 1: a generic lambda — operator() is a template, nothing to deduce.
using generic_handler = decltype([](auto) {});

// Negative case 2: an overloaded operator() — two signatures, deduction is ambiguous.
struct overloaded_handler
{
    void operator()(const request_t &) const {}
    void operator()(const response_t &) const {}
};

static_assert(deducible_handler<concrete_handler>,
              "a concrete non-generic handler must be deducible");
static_assert(!deducible_handler<generic_handler>,
              "a generic lambda must NOT be deducible (spell Sig explicitly)");
static_assert(!deducible_handler<overloaded_handler>,
              "an overloaded operator() must NOT be deducible (spell Sig explicitly)");

// The deduced signature recovers Res(Req) from the (const Req&) -> expected<Res, ...> shape.
static_assert(std::is_same_v<plexus::detail::handler_signature_t<concrete_handler>,
                             response_t(request_t)>);

// node.serve<Family>(name, handler) and node.subscribe<Family>(topic, cb) gate on the SAME
// deducible_handler concept asserted above: the "spell Sig explicitly" static_assert in each
// factory fires for exactly the generic-lambda / overloaded-operator() callables the concept
// rejects, and admits the concrete handler the round-trip below exercises.
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_transport;
using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

template<typename T>
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

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::discovery::static_discovery;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                     std::chrono::milliseconds(2000), std::nullopt,
                                                     std::nullopt};
    opts.redial_seed  = 0x5E14Eu;
    opts.dial_eagerly = eager;
    return opts;
}

struct net
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery   disc{{}};

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

TEST_CASE("serve deduction: concrete handlers deduce, generic and overloaded do not",
          "[node][typed]")
{
    STATIC_REQUIRE(deducible_handler<concrete_handler>);
    STATIC_REQUIRE_FALSE(deducible_handler<generic_handler>);
    STATIC_REQUIRE_FALSE(deducible_handler<overloaded_handler>);
}

TEST_CASE("serve deduction: the deduced serve/caller pair round-trips", "[node][typed]")
{
    net n;
    n.connect();

    auto proc = n.b.serve<echo_codec>(
            "rpc", [](const request_t &req) -> plexus::expected<response_t, std::error_code>
            { return response_t{req.value + 1}; });
    auto call = n.a.caller<response_t(request_t), echo_codec>("rpc");
    n.drive();

    std::optional<std::uint32_t> got;
    call.call(request_t{41},
              [&](plexus::expected<response_t, std::error_code> r)
              {
                  REQUIRE(static_cast<bool>(r));
                  got = r.value().value;
              });
    n.drive();
    REQUIRE(got == 42u);
}
