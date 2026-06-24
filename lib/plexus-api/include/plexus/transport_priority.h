#ifndef HPP_GUARD_PLEXUS_TRANSPORT_PRIORITY_H
#define HPP_GUARD_PLEXUS_TRANSPORT_PRIORITY_H

#include <tuple>
#include <cstddef>
#include <type_traits>

namespace plexus {

template<typename... Ordered>
struct transport_priority
{
};

namespace detail {

template<typename T, typename... Pack>
inline constexpr std::size_t count_in = (0 + ... + (std::is_same_v<T, Pack> ? 1 : 0));

// The two symmetric folds together reject a forgotten transport (count 0), a duplicate
// (count 2), and an extra/unknown type (size mismatch).
template<typename Ordered, typename Declared>
struct is_permutation_of;

template<typename... Ordered, typename... Declared>
struct is_permutation_of<transport_priority<Ordered...>, std::tuple<Declared...>>
        : std::bool_constant<sizeof...(Ordered) == sizeof...(Declared) && ((count_in<Declared, Ordered...> == 1) && ...) && ((count_in<Ordered, Declared...> == 1) && ...)>
{
};

}

template<typename NodeType>
struct priority_builder
{
    template<typename... Ordered>
    static constexpr transport_priority<Ordered...> create() noexcept
    {
        static_assert(detail::is_permutation_of<transport_priority<Ordered...>, typename NodeType::transport_tuple>::value,
                      "plexus: transport_priority<...> must be a permutation of the node's transport "
                      "pack — every declared transport listed exactly once, no forgotten transport, "
                      "no duplicate, no unknown type");
        return {};
    }
};

}

#endif
