#include "plexus/io/message_info.h"
#include "plexus/publisher_gid.h"
#include "plexus/session_id.h"
#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>

using plexus::node_id;
using plexus::publisher_gid;
using plexus::session_id;
using plexus::io::message_info;

namespace {

constexpr node_id id_with_tail(std::uint8_t tail) noexcept
{
    node_id id{};
    id[15] = std::byte{tail};
    return id;
}

}

// --- The three-type distinctness proof: no implicit conversion among them. ---

static_assert(!std::is_convertible_v<publisher_gid, node_id>);
static_assert(!std::is_convertible_v<node_id, publisher_gid>);
static_assert(!std::is_convertible_v<session_id, node_id>);
static_assert(!std::is_convertible_v<node_id, session_id>);
static_assert(!std::is_convertible_v<publisher_gid, session_id>);
static_assert(!std::is_convertible_v<session_id, publisher_gid>);
static_assert(!std::is_convertible_v<session_id, std::uint64_t>);
static_assert(!std::is_convertible_v<std::uint64_t, session_id>);

TEST_CASE("identity_types: publisher_gid exposes its node_id and endpoint counter",
          "[io][identity]")
{
    const auto          node = id_with_tail(0x7);
    const publisher_gid gid{node, 42};
    CHECK(gid.node_id() == node);
    CHECK(gid.endpoint_counter() == 42);
}

TEST_CASE("identity_types: publisher_gid compares by node_id then counter", "[io][identity]")
{
    const auto          node = id_with_tail(0x1);
    const publisher_gid a{node, 1};
    const publisher_gid b{node, 1};
    const publisher_gid c{node, 2};

    CHECK(a == b);
    CHECK(a != c);
    CHECK(a < c);
    CHECK(c > a);
}

TEST_CASE("identity_types: publisher_gid orders by the node_id half first", "[io][identity]")
{
    const publisher_gid low{id_with_tail(0x1), 9999};
    const publisher_gid high{id_with_tail(0x2), 0};
    CHECK(low < high);
}

TEST_CASE("identity_types: session_id wraps a raw u64 and compares by value", "[io][identity]")
{
    const session_id a{0xDEADBEEFCAFEBABEULL};
    const session_id b{0xDEADBEEFCAFEBABEULL};
    const session_id c{1};

    CHECK(a.value() == 0xDEADBEEFCAFEBABEULL);
    CHECK(a == b);
    CHECK(a != c);
    CHECK(c < a);
}

TEST_CASE("identity_types: message_info default-constructs with absent source identity",
          "[io][identity]")
{
    const message_info info{};
    CHECK(info.source_identity == std::nullopt);
    CHECK(info.publication_sequence == 0);
    CHECK(info.source_timestamp == 0);
    CHECK(info.reception_timestamp == 0);
    CHECK(info.from_intra_process == false);
}

TEST_CASE("identity_types: message_info carries an explicit source identity when known",
          "[io][identity]")
{
    const publisher_gid gid{id_with_tail(0x3), 7};
    message_info        info{};
    info.source_identity = gid;
    REQUIRE(info.source_identity.has_value());
    CHECK(info.source_identity->endpoint_counter() == 7);
}
