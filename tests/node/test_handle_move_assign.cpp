// The typed endpoint handles (subscriber<Codec> and value_logger) own their decode/format state in
// a heap block the node references by raw pointer. Move-ASSIGNMENT would free that block while the
// registered adapters still point at it, so it is deleted: the misuse is a compile error rather than
// a use-after-free. Move-CONSTRUCTION stays valid (the unique_ptr transfers the same block, the raw
// adapters stay valid), so container storage keeps working — proven end to end below.

#include "plexus/procedure.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/value_logger.h"
#include "plexus/graph_change_handle.h"

#include "plexus/wire/rpc_status.h"

#include "test_self_loopback_common.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <sstream>
#include <utility>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <system_error>

using plexus_test::sample;
using plexus_test::fixture;
using plexus_test::counting_codec;

namespace plexus_test {

std::ostream &operator<<(std::ostream &os, const sample &s)
{
    return os << s.value;
}

}

namespace {

template<typename>
using sample_family = counting_codec;

using bytes_procedure  = plexus::procedure<>;
using typed_procedure  = plexus::procedure<sample(sample), sample_family>;
using typed_subscriber = plexus::subscriber<counting_codec>;
using typed_logger     = plexus::value_logger<counting_codec, plexus::no_projection>;

static_assert(std::is_move_constructible_v<typed_subscriber>);
static_assert(!std::is_move_assignable_v<typed_subscriber>);
static_assert(!std::is_copy_constructible_v<typed_subscriber>);

static_assert(std::is_move_constructible_v<typed_logger>);
static_assert(!std::is_move_assignable_v<typed_logger>);
static_assert(!std::is_copy_constructible_v<typed_logger>);

static_assert(std::is_move_constructible_v<bytes_procedure>);
static_assert(!std::is_move_assignable_v<bytes_procedure>);
static_assert(std::is_move_constructible_v<typed_procedure>);
static_assert(!std::is_move_assignable_v<typed_procedure>);

static_assert(std::is_move_constructible_v<plexus::graph_change_handle>);
static_assert(!std::is_move_assignable_v<plexus::graph_change_handle>);

void reply_ok(std::span<const std::byte>, bytes_procedure::reply_fn &reply)
{
    reply(plexus::wire::rpc_status::success, {});
}

plexus::expected<sample, std::error_code> echo(const sample &request)
{
    return request;
}

template<typename Procedure, typename Handler>
void pin_procedure_retires_once(Handler handler)
{
    fixture f;
    std::optional<Procedure> moved;
    {
        Procedure first{f.node(), "rpc", handler};
        moved.emplace(std::move(first));
    }
    REQUIRE_THROWS_AS((Procedure{f.node(), "rpc", handler}), std::runtime_error);
    moved.reset();
    REQUIRE_NOTHROW((Procedure{f.node(), "rpc", handler}));
}

template<typename Make>
void pin_subscription_retires_once(fixture &f, Make make)
{
    std::optional<decltype(make())> moved;
    {
        auto first = make();
        moved.emplace(std::move(first));
    }
    f.drive();
    REQUIRE(f.node().count_subscribers("topic") == 1);
    moved.reset();
    f.drive();
    REQUIRE(f.node().count_subscribers("topic") == 0);
}

}

TEST_CASE("handle_move_assign: a move-constructed typed subscriber still delivers", "[node][handles]")
{
    fixture f;

    std::vector<std::uint32_t> got;
    typed_subscriber first{f.node(), "topic", [&](const sample &v) { got.push_back(v.value); }};

    // Relocate the live demand as a container push_back would: the moved-into handle owns the same
    // decode block, and the node's raw-pointer adapters stay valid.
    typed_subscriber moved{std::move(first)};

    plexus::publisher<counting_codec> p{f.node(), "topic"};
    f.drive();

    auto loan = p.borrow();
    REQUIRE(loan);
    loan->value = 0xABCDu;
    p.publish(std::move(loan));
    f.drive();

    REQUIRE(got.size() == 1);
    REQUIRE(got.front() == 0xABCDu);
}

TEST_CASE("handle_move_assign: a procedure cannot be move-assigned, so assignment cannot leak its registration", "[node][handles]")
{
    CHECK(!std::is_move_assignable_v<bytes_procedure>);
    CHECK(!std::is_move_assignable_v<typed_procedure>);
}

TEST_CASE("handle_move_assign: a moved-from procedure retires nothing and the moved-to retires once", "[node][handles]")
{
    pin_procedure_retires_once<bytes_procedure>(&reply_ok);
    pin_procedure_retires_once<typed_procedure>(&echo);
}

TEST_CASE("handle_move_assign: a moved-from subscriber retires nothing and the moved-to retires once", "[node][handles]")
{
    fixture f;
    pin_subscription_retires_once(f, [&] { return plexus::subscriber<>{f.node(), "topic", [](std::span<const std::byte>) {}}; });
    pin_subscription_retires_once(f, [&] { return typed_subscriber{f.node(), "topic", [](const sample &) {}}; });
}

TEST_CASE("handle_move_assign: a moved-from value_logger retires nothing and the moved-to retires once", "[node][handles]")
{
    fixture f;
    std::ostringstream out;
    pin_subscription_retires_once(f, [&] { return typed_logger{f.node(), "topic", plexus::value_logger_options{.out = out}}; });
}

TEST_CASE("handle_move_assign: a moved-from graph_change_handle retires nothing and the moved-to retires once", "[node][handles]")
{
    fixture f;
    f.drive();
    std::uint32_t fired = 0;
    std::optional<plexus::graph_change_handle> moved;
    {
        plexus::graph_change_handle first = f.node().on_graph_change([&](std::uint64_t) { ++fired; });
        moved.emplace(std::move(first));
    }
    plexus::publisher<counting_codec> declared{f.node(), "declared"};
    f.drive();
    REQUIRE(fired == 1);
    moved.reset();
    plexus::publisher<counting_codec> after{f.node(), "after"};
    f.drive();
    REQUIRE(fired == 1);
}
