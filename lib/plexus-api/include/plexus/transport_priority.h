#ifndef HPP_GUARD_PLEXUS_TRANSPORT_PRIORITY_H
#define HPP_GUARD_PLEXUS_TRANSPORT_PRIORITY_H

#include <tuple>
#include <cstddef>
#include <type_traits>

namespace plexus {

// The reordered transport preference. A pure type-level tag carrying no state; delivered as a
// ctor tag and read by type only.
template<typename... Ordered>
struct transport_priority
{
};

namespace detail {

// How many times T appears in a pack.
template<typename T, typename... Pack>
inline constexpr std::size_t count_in = (0 + ... + (std::is_same_v<T, Pack> ? 1 : 0));

// Ordered... is a permutation of Declared... iff the sizes match AND every declared type
// appears exactly once among Ordered AND every ordered type appears exactly once among
// Declared. The two symmetric folds together reject a forgotten transport (count 0), a
// duplicate (count 2), and an extra/unknown type (size mismatch + the symmetric fold).
template<typename Ordered, typename Declared>
struct is_permutation_of;

template<typename... Ordered, typename... Declared>
struct is_permutation_of<transport_priority<Ordered...>, std::tuple<Declared...>>
        : std::bool_constant<sizeof...(Ordered) == sizeof...(Declared) &&
                             ((count_in<Declared, Ordered...> == 1) && ...) &&
                             ((count_in<Ordered, Declared...> == 1) && ...)>
{
};

}

// The builder: NodeType pins the declared transport pack; create<Ordered...>() validates the
// ordering is a permutation of that pack at compile time and mints the tag.
template<typename NodeType>
struct priority_builder
{
    template<typename... Ordered>
    static constexpr transport_priority<Ordered...> create() noexcept
    {
        static_assert(
                detail::is_permutation_of<transport_priority<Ordered...>,
                                          typename NodeType::transport_tuple>::value,
                "plexus: transport_priority<...> must be a permutation of the node's transport "
                "pack — every declared transport listed exactly once, no forgotten transport, "
                "no duplicate, no unknown type");
        return {};
    }
};

}

#endif
