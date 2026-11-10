// The injected-codec concept vocabulary: a hand-rolled codec satisfies typed_codec, a type_info() member makes it
// identity_bearing, and resolve_identity prefers an explicit override over the codec's own. The codecs are hand-rolled
// here; plexus never names a serializer library.

#include "plexus/expected.h"
#include "plexus/typed_codec.h"
#include "plexus/with_identity.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>

namespace {

struct value_t
{
    std::uint32_t v{};
};

// A minimal codec with NO type_info(): satisfies typed_codec but not identity_bearing.
struct plain_codec
{
    using value_type = value_t;

    plexus::wire_bytes<> encode(const value_t &) const
    {
        return {};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, value_t &) const
    {
        return {};
    }
};

// The same codec PLUS a type_info(): now identity_bearing.
struct identified_codec
{
    using value_type = value_t;

    plexus::wire_bytes<> encode(const value_t &) const
    {
        return {};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, value_t &) const
    {
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0xABCDu, "value_t"};
    }
};

static_assert(plexus::typed_codec<plain_codec>, "a minimal encode/decode codec satisfies typed_codec");
static_assert(plexus::typed_codec<identified_codec>, "adding type_info does not break typed_codec");

static_assert(!plexus::identity_bearing<plain_codec>, "a codec without type_info is not identity_bearing");
static_assert(plexus::identity_bearing<identified_codec>, "a codec with type_info() -> type_identity is identity_bearing");

}

TEST_CASE("resolve_identity uses an identity_bearing codec's type_info when no argument is given", "[node][typed][concepts]")
{
    identified_codec codec;
    const auto resolved = plexus::resolve_identity(codec, std::nullopt);
    REQUIRE(resolved.type_id == 0xABCDu);
}

TEST_CASE("resolve_identity prefers an explicit argument over the codec's type_info", "[node][typed][concepts]")
{
    identified_codec codec;
    const auto resolved = plexus::resolve_identity(codec, plexus::type_identity{0x1234u, "override"});
    REQUIRE(resolved.type_id == 0x1234u);
}

TEST_CASE("resolve_identity resolves a codec adapted by with_identity to the identity it was given", "[node][typed][concepts]")
{
    plexus::with_identity codec{plain_codec{}, plexus::type_identity{0x5678u, "adapted"}};
    const auto resolved = plexus::resolve_identity(codec, std::nullopt);
    REQUIRE(resolved.type_id == 0x5678u);
}
