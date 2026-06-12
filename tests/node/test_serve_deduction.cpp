// The negative-compile probe for the deduced serve form. node.serve<Family>(handler) will
// deduce the request/response types from a handler with a SINGLE concrete call signature;
// a generic lambda ([](auto){}) or an overloaded-operator() struct has no single signature
// to deduce, so the family must reject it with a "spell Sig explicitly" diagnostic rather
// than silently mis-deduce. This TU stands the assertion scaffold up now, against a local
// `deducible` concept stub mirroring the shape the real detail::deducible_handler will take;
// the family wave repoints the positive/negative cases at the real concept.

#include "plexus/expected.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <system_error>
#include <type_traits>

namespace {

// A non-generic callable has exactly ONE operator() with a deducible argument type. We probe
// deducibility the way a serve-form deduction would: ask whether &F::operator() names a
// single non-template member (a generic lambda's operator() is a template, so taking its
// address as a concrete pointer-to-member fails; an overloaded operator() is ambiguous, so
// the address is ill-formed too). The concept is satisfied ONLY when that address is a
// well-formed concrete pointer-to-member.
template <typename F>
concept deducible = requires { static_cast<void>(&F::operator()); };

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
    decltype([](const request_t &) -> plexus::expected<response_t, std::error_code> {
        return response_t{0};
    });

// Negative case 1: a generic lambda — operator() is a template, nothing to deduce.
using generic_handler = decltype([](auto) {});

// Negative case 2: an overloaded operator() — two signatures, deduction is ambiguous.
struct overloaded_handler
{
    void operator()(const request_t &) const {}
    void operator()(const response_t &) const {}
};

static_assert(deducible<concrete_handler>,
              "a concrete non-generic handler must be deducible");
static_assert(!deducible<generic_handler>,
              "a generic lambda must NOT be deducible (spell Sig explicitly)");
static_assert(!deducible<overloaded_handler>,
              "an overloaded operator() must NOT be deducible (spell Sig explicitly)");

}

TEST_CASE("serve deduction: concrete handlers deduce, generic and overloaded do not", "[node][typed]")
{
    STATIC_REQUIRE(deducible<concrete_handler>);
    STATIC_REQUIRE_FALSE(deducible<generic_handler>);
    STATIC_REQUIRE_FALSE(deducible<overloaded_handler>);
}
