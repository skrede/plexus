// The fail_closed throw-semantics oracle. On the PC/server build (exceptions enabled),
// fail_closed throws std::runtime_error carrying the supplied message — byte-identical to
// the bare throw the fail-closed contracts used before the seam. The [[noreturn]] contract
// is asserted statically (a call type-checks as never-returning), never by actually entering
// the abort branch. No socket, no backend — header-only core.

#include "plexus/detail/fail_closed.h"

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>
#include <string>
#include <type_traits>

using plexus::detail::fail_closed;

// [[noreturn]] proof: the attribute is part of the declared type, so a function returning
// non-void whose only body is a fail_closed call must compile (the [[noreturn]] call covers
// the missing return). If fail_closed lost [[noreturn]], this helper would warn/err under
// -Werror on the missing return. The helper is never invoked — the proof is at compile time.
[[maybe_unused]] static int never_returns_proof()
{
    fail_closed("unreachable");
}

TEST_CASE("fail_closed throws runtime_error under exceptions", "[fnx][fail_closed]")
{
    // Call through a non-[[noreturn]] lambda so Catch2's post-expression bookkeeping is not
    // provably unreachable under MSVC (C4702); the throw still propagates identically.
    const auto call = [] { fail_closed("x"); };
    REQUIRE_THROWS_AS(call(), std::runtime_error);
}

TEST_CASE("fail_closed propagates the contract message", "[fnx][fail_closed]")
{
    try
    {
        fail_closed("degraded RNG");
#if !defined(_MSC_VER)
        // Unreachable given fail_closed is [[noreturn]] — a guard against it silently returning
        // if that contract were lost. MSVC proves it dead (C4702); the catch below carries the
        // real assertion on every compiler.
        FAIL("fail_closed must not return under exceptions");
#endif
    }
    catch(const std::runtime_error &e)
    {
        REQUIRE(std::string{e.what()} == "degraded RNG");
    }
}
