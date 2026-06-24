#ifndef HPP_GUARD_PLEXUS_DETAIL_FUNCTION_TRAITS_H
#define HPP_GUARD_PLEXUS_DETAIL_FUNCTION_TRAITS_H

#include "plexus/expected.h"

#include <system_error>
#include <type_traits>

namespace plexus::detail {

// A handler is deducible when &F::operator() names a single concrete member function: a
// generic lambda's operator() is a template (its address as a concrete member pointer is
// ill-formed) and an overloaded operator() is ambiguous, so both fail this requirement.
// The serve/subscribe/caller factories gate on this and emit the "spell Sig explicitly"
// diagnostic.
template<typename F>
concept deducible_handler = requires { static_cast<void>(&F::operator()); };

// Function-traits over a callable's operator() (the Boost.CallableTraits / function_traits
// technique, not a dependency): a partial specialization per member-pointer qualifier form
// decomposes one concrete operator() into its result type and its single argument. The
// canonical const / non-const / noexcept / & / && qualifier set is covered in full — a
// missing form would silently leave a valid lambda non-deducible. The primary is
// left undefined so a non-deducible callable (generic or overloaded operator()) names no
// member and fails the concept rather than mis-deducing.
template<typename F>
struct callable_traits;

#define PLEXUS_CALLABLE_TRAITS(QUALIFIERS)                                                                                                                                              \
    template<typename R, typename C, typename A>                                                                                                                                        \
    struct callable_traits<R (C::*)(A) QUALIFIERS>                                                                                                                                      \
    {                                                                                                                                                                                   \
        using result_type   = R;                                                                                                                                                        \
        using argument_type = A;                                                                                                                                                        \
    };

PLEXUS_CALLABLE_TRAITS()
PLEXUS_CALLABLE_TRAITS(const)
PLEXUS_CALLABLE_TRAITS(noexcept)
PLEXUS_CALLABLE_TRAITS(const noexcept)
PLEXUS_CALLABLE_TRAITS(&)
PLEXUS_CALLABLE_TRAITS(const &)
PLEXUS_CALLABLE_TRAITS(& noexcept)
PLEXUS_CALLABLE_TRAITS(const & noexcept)
PLEXUS_CALLABLE_TRAITS(&&)
PLEXUS_CALLABLE_TRAITS(const &&)
PLEXUS_CALLABLE_TRAITS(&& noexcept)
PLEXUS_CALLABLE_TRAITS(const && noexcept)

#undef PLEXUS_CALLABLE_TRAITS

// The first argument of a callable's operator(), regardless of arity: a subscriber
// callback is either void(const T&) or void(const T&, message_info), and only its leading
// value parameter carries the deducible type T. A per-qualifier specialization captures the
// leading argument and ignores any trailing parameters. The primary is undefined so a
// non-deducible callback fails the concept rather than mis-deducing.
template<typename F>
struct leading_argument;

#define PLEXUS_LEADING_ARGUMENT(QUALIFIERS)                                                                                                                                             \
    template<typename R, typename C, typename A0, typename... Rest>                                                                                                                     \
    struct leading_argument<R (C::*)(A0, Rest...) QUALIFIERS>                                                                                                                           \
    {                                                                                                                                                                                   \
        using type = A0;                                                                                                                                                                \
    };

PLEXUS_LEADING_ARGUMENT()
PLEXUS_LEADING_ARGUMENT(const)
PLEXUS_LEADING_ARGUMENT(noexcept)
PLEXUS_LEADING_ARGUMENT(const noexcept)
PLEXUS_LEADING_ARGUMENT(&)
PLEXUS_LEADING_ARGUMENT(const &)
PLEXUS_LEADING_ARGUMENT(& noexcept)
PLEXUS_LEADING_ARGUMENT(const & noexcept)
PLEXUS_LEADING_ARGUMENT(&&)
PLEXUS_LEADING_ARGUMENT(const &&)
PLEXUS_LEADING_ARGUMENT(&& noexcept)
PLEXUS_LEADING_ARGUMENT(const && noexcept)

#undef PLEXUS_LEADING_ARGUMENT

// Map a concrete subscriber callback F — operator() of the shape (const T&) or
// (const T&, message_info) — to the value type T it carries (reference and const stripped).
template<typename F>
    requires deducible_handler<F>
using subscriber_value_t = std::remove_cvref_t<typename leading_argument<decltype(&F::operator())>::type>;

// The deduced response type carried by an RPC handler's return: the Res of an
// expected<Res, std::error_code>. Left undefined for any other return type so a handler
// that does not return the expected-shaped result is rejected at the point of deduction.
template<typename R>
struct rpc_response;

template<typename Res>
struct rpc_response<expected<Res, std::error_code>>
{
    using type = Res;
};

// Map a concrete RPC handler F — operator() of the shape (const Req&) ->
// expected<Res, std::error_code> — to the signature Res(Req) the procedure/caller
// templates decompose. The argument's reference and const are stripped to recover Req; the
// return's expected is unwrapped to recover Res.
template<typename F>
    requires deducible_handler<F>
using handler_signature_t = typename rpc_response<typename callable_traits<decltype(&F::operator())>::result_type>::type(
        std::remove_cvref_t<typename callable_traits<decltype(&F::operator())>::argument_type>);

}

#endif
