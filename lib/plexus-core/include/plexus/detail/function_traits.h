#ifndef HPP_GUARD_PLEXUS_DETAIL_FUNCTION_TRAITS_H
#define HPP_GUARD_PLEXUS_DETAIL_FUNCTION_TRAITS_H

#include "plexus/expected.h"

#include <system_error>
#include <type_traits>

namespace plexus::detail {

// Deducible only when &F::operator() names a single concrete member function:
// a generic or overloaded operator() fails this and is rejected upstream.
template<typename F>
concept deducible_handler = requires { static_cast<void>(&F::operator()); };

// The Boost.CallableTraits technique (not a dependency): one specialization per
// member-pointer qualifier form. The primary is left undefined so a non-deducible
// callable names no member and fails rather than mis-deducing.
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

// The leading argument of a callable's operator(), ignoring trailing parameters.
// The primary is undefined so a non-deducible callback fails rather than mis-deducing.
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

template<typename F>
    requires deducible_handler<F>
using subscriber_value_t = std::remove_cvref_t<typename leading_argument<decltype(&F::operator())>::type>;

// Left undefined for any return other than expected<Res, std::error_code> so a
// handler with a non-expected return is rejected at deduction.
template<typename R>
struct rpc_response;

template<typename Res>
struct rpc_response<expected<Res, std::error_code>>
{
    using type = Res;
};

template<typename F>
    requires deducible_handler<F>
using handler_signature_t = typename rpc_response<typename callable_traits<decltype(&F::operator())>::result_type>::type(
        std::remove_cvref_t<typename callable_traits<decltype(&F::operator())>::argument_type>);

}

#endif
