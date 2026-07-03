// The typed endpoint handles (subscriber<Codec> and value_logger) own their decode/format state in
// a heap block the node references by raw pointer. Move-ASSIGNMENT would free that block while the
// registered adapters still point at it, so it is deleted: the misuse is a compile error rather than
// a use-after-free. Move-CONSTRUCTION stays valid (the unique_ptr transfers the same block, the raw
// adapters stay valid), so container storage keeps working — proven end to end below.

#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/value_logger.h"

#include "test_self_loopback_common.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>
#include <cstdint>
#include <utility>
#include <type_traits>

using plexus_test::sample;
using plexus_test::fixture;
using plexus_test::counting_codec;

namespace {

using typed_subscriber = plexus::subscriber<counting_codec>;
using typed_logger     = plexus::value_logger<counting_codec, plexus::no_projection>;

static_assert(std::is_move_constructible_v<typed_subscriber>);
static_assert(!std::is_move_assignable_v<typed_subscriber>);
static_assert(!std::is_copy_constructible_v<typed_subscriber>);

static_assert(std::is_move_constructible_v<typed_logger>);
static_assert(!std::is_move_assignable_v<typed_logger>);
static_assert(!std::is_copy_constructible_v<typed_logger>);

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
