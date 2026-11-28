#include "plexus/detail/expected.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdint>
#include <string_view>
#include <type_traits>

namespace {

struct explicit_from_int
{
    explicit explicit_from_int(std::int32_t)
    {
    }
};

struct implicit_from_int
{
    implicit_from_int(std::int32_t)
    {
    }
};

struct failure
{
    std::int32_t code;
};

using explicit_result = plexus::detail::expected<explicit_from_int, failure>;
using implicit_result = plexus::detail::expected<implicit_from_int, failure>;
using string_result   = plexus::detail::expected<std::string, failure>;

static_assert(std::is_constructible_v<explicit_result, std::int32_t>);
static_assert(!std::is_convertible_v<std::int32_t, explicit_result>);
static_assert(std::is_convertible_v<std::int32_t, implicit_result>);
static_assert(std::is_convertible_v<explicit_from_int, explicit_result>);
static_assert(std::is_convertible_v<plexus::detail::unexpected<failure>, explicit_result>);
static_assert(std::is_convertible_v<plexus::detail::unexpected<failure>, plexus::detail::expected<void, failure>>);
static_assert(!std::is_convertible_v<std::string_view, string_result>);
static_assert(std::is_constructible_v<string_result, std::string_view>);

}

TEST_CASE("expected converts implicitly only where its value type does", "[expected]")
{
    const implicit_result copied = std::int32_t{7};
    const explicit_result direct(std::int32_t{7});
    CHECK(copied.has_value());
    CHECK(direct.has_value());
}
