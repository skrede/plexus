// The move_only_function fallback oracle: pins the C++20-floor fallback in
// detail/compat.h (the path the gnu++20 PC build takes today, since
// __cpp_lib_move_only_function is a C++23 library feature). It proves the SBO
// inline storage allocates ZERO heap for a small callable, spills a large one to
// the heap, and keeps the move-only / null / bool / invoke semantics byte-stable.
// The alloc_counter override lives in THIS TU (operator new/delete are replaceable
// at most once per binary), so this is the only TU of its executable.

#include "plexus/detail/compat.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <memory>
#include <utility>

using plexus::detail::move_only_function;
using plexus::testing::alloc_count;

namespace {

// A small captureless-equivalent callable: one captured int (8 bytes after decay)
// — well within the measured SBO budget, so wrapping it must not touch the heap.
struct small_adder
{
    int base;
    int operator()(int x) const { return base + x; }
};

// A large callable whose captured state exceeds the SBO inline buffer, forcing the
// fallback to spill to a heap allocation. The 256-byte payload is deliberately over
// the measured k_sbo_size so the spill path is exercised.
struct large_callable
{
    std::array<std::byte, 256> payload{};
    int                        base = 7;
    int operator()(int x) const { return base + x; }
};

}

TEST_CASE("move_only_function invokes a small callable with zero heap", "[io][move_only_function]")
{
    plexus::testing::reset_alloc_count();
    const std::size_t before = alloc_count();

    move_only_function<int(int)> fn{small_adder{.base = 10}};

    const std::size_t after = alloc_count();
    REQUIRE(after - before == 0);
    REQUIRE(static_cast<bool>(fn));
    REQUIRE(fn(5) == 15);
    REQUIRE(fn(5) == 15);
}

TEST_CASE("move_only_function spills a large callable to the heap and invokes it",
          "[io][move_only_function]")
{
    move_only_function<int(int)> fn{large_callable{}};

    REQUIRE(static_cast<bool>(fn));
    REQUIRE(fn(3) == 10);
}

TEST_CASE("move_only_function admits a move-only callable", "[io][move_only_function]")
{
    auto                         owned = std::make_unique<int>(40);
    move_only_function<int(int)> fn{[p = std::move(owned)](int x) { return *p + x; }};

    REQUIRE(static_cast<bool>(fn));
    REQUIRE(fn(2) == 42);
}

TEST_CASE("move_only_function default and null construct empty", "[io][move_only_function]")
{
    move_only_function<int(int)> def;
    move_only_function<int(int)> null{nullptr};

    REQUIRE_FALSE(static_cast<bool>(def));
    REQUIRE_FALSE(static_cast<bool>(null));
    REQUIRE(def == nullptr);
    REQUIRE(nullptr == null);
    REQUIRE_FALSE(def != nullptr);
}

TEST_CASE("move_only_function move-construct transfers the target and empties the source",
          "[io][move_only_function]")
{
    move_only_function<int(int)> src{small_adder{.base = 100}};
    move_only_function<int(int)> dst{std::move(src)};

    REQUIRE(static_cast<bool>(dst));
    REQUIRE_FALSE(static_cast<bool>(src)); // NOLINT(bugprone-use-after-move)
    REQUIRE(dst(1) == 101);
}

TEST_CASE("move_only_function move-assign transfers the target and empties the source",
          "[io][move_only_function]")
{
    move_only_function<int(int)> src{small_adder{.base = 200}};
    move_only_function<int(int)> dst{small_adder{.base = 1}};

    dst = std::move(src);

    REQUIRE(static_cast<bool>(dst));
    REQUIRE_FALSE(static_cast<bool>(src)); // NOLINT(bugprone-use-after-move)
    REQUIRE(dst(2) == 202);
}

TEST_CASE("move_only_function move transfers a heap-spilled target", "[io][move_only_function]")
{
    move_only_function<int(int)> src{large_callable{}};
    move_only_function<int(int)> dst{std::move(src)};

    REQUIRE(static_cast<bool>(dst));
    REQUIRE_FALSE(static_cast<bool>(src)); // NOLINT(bugprone-use-after-move)
    REQUIRE(dst(3) == 10);
}
