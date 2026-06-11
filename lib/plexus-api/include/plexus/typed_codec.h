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

// The wire identity a typed endpoint carries for its value type: a deterministic
// 64-bit tag (the matching authority) plus a human-readable name (diagnostics only).
struct type_identity
{
    std::uint64_t    type_id{};
    std::string_view type_name{};
};

// An injected, serializer-agnostic codec for a single value type. plexus never names
// a serializer library; a consumer supplies any type satisfying this concept.
//
// encode(const value_type&) returns a wire_bytes<> whose backing storage need only
// outlive the SYNCHRONOUS publish/call it feeds — the egress copies the bytes into its
// own scratch before the verb returns, so the wire_bytes owner is forward-compatibility
// for a future non-copying egress, not a live retention obligation.
//
// decode(span, value_type&) writes into the caller's slot and reports success/failure
// through expected<void, std::error_code>. A view-type value_type (one that aliases the
// passed span rather than copying) is valid for the delivery callback's duration ONLY;
// deferred consumption must copy the value out before the callback returns.
template <typename C>
concept typed_codec = requires(C &c, const typename C::value_type &v,
                               std::span<const std::byte> bytes,
                               typename C::value_type &slot) {
    typename C::value_type;
    { c.encode(v) } -> std::convertible_to<wire_bytes<>>;
    { c.decode(bytes, slot) } -> std::same_as<expected<void, std::error_code>>;
};

// A codec that also publishes its value type's wire identity. When present it supplies
// the type_id a typed endpoint gates on without the consumer restating it.
template <typename C>
concept identity_bearing = requires(const C &c) {
    { c.type_info() } -> std::convertible_to<type_identity>;
};

// Resolve a typed endpoint's wire identity under a fixed precedence. An EXPLICIT
// type_identity argument always wins (this overload accepts any codec). With NO explicit
// argument, an identity_bearing codec supplies it; a codec that is neither
// identity_bearing nor handed an explicit identity is a COMPILE ERROR naming both
// remedies. There is NEVER a typeid / RTTI fallback — an unresolved identity is a
// contract gap the consumer must close, not a runtime accident.
template <typename Codec>
constexpr type_identity resolve_identity(const Codec &, type_identity explicit_identity)
{
    return explicit_identity;
}

template <typename Codec>
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

// The std::optional convenience: an engaged optional routes to the explicit overload, an
// empty one to the codec's type_info (which compile-errors for a non-identity-bearing
// codec). The branch is resolved at runtime ONLY when the codec is identity_bearing — a
// non-identity-bearing codec instantiates only the explicit path, so the empty-optional
// case never compiles its (ill-formed) type_info read.
template <typename Codec>
constexpr type_identity resolve_identity(const Codec &codec,
                                         std::optional<type_identity> explicit_identity)
{
    if constexpr(identity_bearing<Codec>)
        return explicit_identity ? *explicit_identity : codec.type_info();
    else
        return resolve_identity(codec, explicit_identity.value());
}

}

#endif
