#ifndef HPP_GUARD_PLEXUS_TYPED_CODEC_H
#define HPP_GUARD_PLEXUS_TYPED_CODEC_H

#include "plexus/expected.h"

#include "plexus/wire_bytes.h"

#include <span>
#include <cstdint>
#include <utility>
#include <optional>
#include <concepts>
#include <string_view>
#include <type_traits>
#include <system_error>

namespace plexus {

// type_id is the matching authority; type_name is diagnostics only.
struct type_identity
{
    std::uint64_t type_id{};
    std::string_view type_name{};
};

// A serializer-agnostic codec for a single value type. encode's returned wire_bytes backing
// need only outlive the synchronous publish/call it feeds — the egress copies the bytes into
// its own scratch before the verb returns. A view-type value_type (one aliasing the decode
// span rather than copying) is valid for the delivery callback's duration only; deferred
// consumption must copy out before the callback returns.
template<typename C>
concept typed_codec = requires(C &c, const typename C::value_type &v, std::span<const std::byte> bytes, typename C::value_type &slot) {
    typename C::value_type;
    { c.encode(v) } -> std::convertible_to<wire_bytes<>>;
    { c.decode(bytes, slot) } -> std::same_as<expected<void, std::error_code>>;
};

template<typename C>
concept identity_bearing = requires(const C &c) {
    { c.type_info() } -> std::convertible_to<type_identity>;
};

// An explicit type_identity argument always wins; otherwise an identity_bearing codec
// supplies it. There is no typeid / RTTI fallback — an unresolved identity is a compile error.
template<typename Codec>
constexpr type_identity resolve_identity(const Codec &, type_identity explicit_identity)
{
    return explicit_identity;
}

template<typename Codec>
constexpr type_identity resolve_identity(const Codec &codec)
{
    static_assert(identity_bearing<Codec>,
                  "plexus: a typed endpoint needs a type identity — either pass an "
                  "explicit type_identity argument, or give the codec a "
                  "type_info() -> type_identity member. There is no typeid fallback.");
    if constexpr(identity_bearing<Codec>)
        return codec.type_info();
    else
        return {};
}

// A non-identity-bearing codec instantiates only the explicit path, so its empty-optional case
// never compiles the (ill-formed) type_info read.
template<typename Codec>
constexpr type_identity resolve_identity(const Codec &codec, std::optional<type_identity> explicit_identity)
{
    if constexpr(identity_bearing<Codec>)
        return explicit_identity ? *explicit_identity : codec.type_info();
    else
        return resolve_identity(codec, explicit_identity.value());
}

}

#endif
