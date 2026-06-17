#ifndef HPP_GUARD_PLEXUS_SUBSCRIBER_H
#define HPP_GUARD_PLEXUS_SUBSCRIBER_H

#include "plexus/node.h"

#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/endpoint_seam.h"

#include "plexus/typed_codec.h"
#include "plexus/recording_qos.h"

#include "plexus/detail/compat.h"

#include <span>
#include <memory>
#include <cstddef>
#include <utility>
#include <optional>
#include <functional>
#include <type_traits>
#include <string_view>
#include <system_error>

namespace plexus {

// The subscribing endpoint family. The primary template names the typed endpoint
// (subscriber<Codec> below); the bytes endpoint is the subscriber<void> specialization —
// subscriber<> selects it via the defaulted Codec (the default lives in node.h's forward
// declaration, seen first), so every bytes spelling keeps compiling unchanged.
template <typename Codec>
class subscriber;

// The typed subscriber's construction-time options. posture is the typed attach gate
// (lenient default attaches to an untyped producer; strict refuses it). on_decode_failure
// is the opt-in escape callback receiving the raw bytes + errc instead of a dropped frame
// — std::optional because absence is meaningful (the default is drop+count+warn alone).
// An explicit type identity overrides the codec's own.
struct typed_subscriber_options
{
    io::subscriber_qos                                                            qos{};
    io::attach_posture                                                            posture =
        io::attach_posture::lenient;
    std::optional<type_identity>                                                  type_id{};
    std::optional<std::function<void(std::span<const std::byte>, std::error_code)>> on_decode_failure{};

    // The per-topic recording-QoS override. std::optional because ABSENCE is meaningful:
    // unset falls back to the node-level recording_qos default, present overrides the
    // recording fidelity for this topic alone (the shm_geometry/publisher precedent).
    std::optional<recording_qos>                                                  capture{};
};

// The bytes subscribing endpoint: the CONSTRUCTOR is the registration — it installs a
// STANDING topic-level demand on the node, which fans the per-peer subscribe to every
// known peer now and to each later-discovered peer with no further user action. The
// callback accepts EITHER a 2-arg `void(span<const std::byte>)` or a 3-arg
// `void(span<const std::byte>, const message_info&)` callable, dispatched at
// construction with if constexpr — a 2-arg callback costs nothing for the metadata it
// ignores.
//
// LIFETIME: a subscriber must NOT outlive its node. The canonical usage is
// member-init aggregation (node ref first, handles after), so reverse destruction
// retires the demand before the node. Dropping the handle retires the demand: it stops
// the callback and, when it was the last local subscriber for the fqn, unsubscribes the
// topic from every fanned peer. A moved-from handle is inert (empty retire); its
// destructor does nothing, so no callback ever fires through a dropped subscriber.
template <>
class subscriber<void>
{
public:
    template <typename Policy, typename... NodeTs, typename Cb>
    subscriber(node<Policy, NodeTs...> &n, std::string_view fqn, Cb cb)
        : subscriber(n, fqn, io::subscriber_qos{}, std::move(cb))
    {
    }

    template <typename Policy, typename... NodeTs, typename Cb>
    subscriber(node<Policy, NodeTs...> &n, std::string_view fqn, const io::subscriber_qos &qos, Cb cb)
    {
        // The arity is the implicit stamp demand: a 2-arg callback provably consumes no
        // message_info, so it never wants the receive-side clock read. Resolved once here
        // (the same if constexpr adapt() keys on) and carried on the LOCAL qos only.
        io::subscriber_qos local_qos = qos;
        local_qos.wants_message_info =
            std::is_invocable_v<Cb &, std::span<const std::byte>, const io::message_info &>;
        io::endpoint_seam seam = n.endpoint_seam_for();
        const auto rid = seam.register_subscriber(seam.ctx, fqn, local_qos, adapt(std::move(cb)),
                                                  std::nullopt, nullptr, io::object_dispatch{},
                                                  std::nullopt);
        m_retire = [seam, rid] { seam.retire_subscriber(seam.ctx, rid); };
    }

    subscriber(subscriber &&) noexcept = default;
    subscriber &operator=(subscriber &&) noexcept = default;

    subscriber(const subscriber &) = delete;
    subscriber &operator=(const subscriber &) = delete;

    ~subscriber()
    {
        if(m_retire != nullptr)
            m_retire();
    }

private:
    // Normalize the user callback to the node's 3-arg demux shape. A 3-arg callable is
    // forwarded the message_info; a 2-arg callable is wrapped to drop it. The arity is
    // resolved ONCE here (the cold registration path), so the hot demux fans a uniform
    // signature with no per-frame branch.
    template <typename Cb>
    static plexus::detail::move_only_function<void(std::span<const std::byte>, const io::message_info &)>
    adapt(Cb cb)
    {
        if constexpr(std::is_invocable_v<Cb &, std::span<const std::byte>, const io::message_info &>)
            return [cb = std::move(cb)](std::span<const std::byte> bytes, const io::message_info &info) mutable
            { cb(bytes, info); };
        else
            return [cb = std::move(cb)](std::span<const std::byte> bytes, const io::message_info &) mutable
            { cb(bytes); };
    }

    plexus::detail::move_only_function<void()> m_retire;
};

// The typed subscribing endpoint: the CONSTRUCTOR installs a standing typed demand. It
// registers BOTH a bytes decode adapter (decode → typed callback; a decode failure is
// dropped+counted+warned, with the opt-in escape callback handed the raw bytes + errc)
// AND a process-tier object-dispatch entry (native-key-checked, recovering the concrete T
// from the carrier and invoking the SAME normalized callback) under ONE registration id,
// so retire is atomic. The callback accepts EITHER `void(const T&)` or
// `void(const T&, const message_info&)`, the arity resolved ONCE at registration.
//
// VIEW-T LIFETIME: a view-type value_type (one aliasing the decode span / the carrier
// slot rather than copying) is valid for the callback invocation ONLY; deferred
// consumption must copy the value out before the callback returns.
//
// LIFETIME: a subscriber must NOT outlive its node (member-init aggregation, node ref
// first). The decode state lives in a heap block the handle owns; the node's stored
// adapters reference it by raw pointer and are retired (removing both entries) before the
// block is freed. A moved-from handle is inert; its destructor does nothing.
template <typename Codec>
class subscriber
{
public:
    using value_type = typename Codec::value_type;
    using typed_callback =
        plexus::detail::move_only_function<void(const value_type &, const io::message_info &)>;

    template <typename Policy, typename... NodeTs, typename Cb>
    subscriber(node<Policy, NodeTs...> &n, std::string_view fqn, Cb cb)
        : subscriber(n, fqn, typed_subscriber_options{}, std::move(cb), Codec{})
    {
    }

    template <typename Policy, typename... NodeTs, typename Cb>
    subscriber(node<Policy, NodeTs...> &n, std::string_view fqn,
               const typed_subscriber_options &opts, Cb cb, Codec codec = {})
        : m_state(std::make_unique<state>(std::move(codec), adapt(std::move(cb)),
                                          opts.on_decode_failure))
    {
        static_assert(typed_codec<Codec>,
                      "plexus: a typed subscriber needs a codec satisfying typed_codec "
                      "(value_type; encode(const value_type&) -> wire_bytes<>; "
                      "decode(span, value_type&) -> expected<void, error_code>).");

        const auto identity = resolve_identity(m_state->codec, opts.type_id);
        io::subscriber_qos qos = opts.qos;
        qos.posture = opts.posture;
        // A 2-arg typed callback consumes no message_info, so it never wants the receive-
        // side clock read; a 3-arg callback wants it UNLESS the qos explicitly opted out.
        // Carried on the LOCAL qos only — never the subscribe wire region.
        qos.wants_message_info =
            std::is_invocable_v<Cb &, const value_type &, const io::message_info &>
            && opts.qos.wants_message_info;

        state *st = m_state.get();
        auto bytes_adapter = [st](std::span<const std::byte> bytes, const io::message_info &info)
        { st->on_bytes(bytes, info); };

        io::object_dispatch dispatch = [st](const io::object_carrier &carrier, const io::message_info &info)
        { st->on_object(carrier, info); };

        io::endpoint_seam seam = n.endpoint_seam_for();
        const auto rid = seam.register_subscriber(
            seam.ctx, fqn, qos, std::move(bytes_adapter), identity.type_id,
            &io::detail::type_key<value_type>, std::move(dispatch),
            opts.capture ? std::optional{opts.capture->to_rule()} : std::nullopt);
        m_retire = [seam, rid] { seam.retire_subscriber(seam.ctx, rid); };
    }

    // The count of inbound frames whose decode failed — dropped, never a partial T. Read
    // through the live handle.
    [[nodiscard]] std::size_t decode_failed() const noexcept { return m_state->decode_failed; }

    subscriber(subscriber &&) noexcept = default;
    subscriber &operator=(subscriber &&) noexcept = default;

    subscriber(const subscriber &) = delete;
    subscriber &operator=(const subscriber &) = delete;

    ~subscriber()
    {
        if(m_retire != nullptr)
            m_retire();
    }

private:
    // Normalize the user callback to the 2-arg typed shape, the arity resolved ONCE here.
    template <typename Cb>
    static typed_callback adapt(Cb cb)
    {
        if constexpr(std::is_invocable_v<Cb &, const value_type &, const io::message_info &>)
            return [cb = std::move(cb)](const value_type &v, const io::message_info &info) mutable
            { cb(v, info); };
        else
            return [cb = std::move(cb)](const value_type &v, const io::message_info &) mutable
            { cb(v); };
    }

    // The heap-stable decode state the node's adapters reference by raw pointer. It owns
    // the codec, the normalized callback, the reused decode slot, the failure counter, and
    // the opt-in escape callback — all cold-path setup, no per-message allocation.
    struct state
    {
        Codec          codec;
        typed_callback callback;
        std::optional<std::function<void(std::span<const std::byte>, std::error_code)>> on_failure;
        value_type     slot{};
        std::size_t    decode_failed{};

        state(Codec c, typed_callback cb,
              std::optional<std::function<void(std::span<const std::byte>, std::error_code)>> fail)
            : codec(std::move(c))
            , callback(std::move(cb))
            , on_failure(std::move(fail))
        {
        }

        // The bytes path: decode into the reused slot. Success invokes the typed callback
        // for its duration; failure increments the counter, hands the escape callback the
        // raw bytes + errc if set, and drops — NEVER a partial T, never a teardown.
        void on_bytes(std::span<const std::byte> bytes, const io::message_info &info)
        {
            auto decoded = codec.decode(bytes, slot);
            if(decoded.has_value())
            {
                callback(slot, info);
                return;
            }
            ++decode_failed;
            if(on_failure)
                (*on_failure)(bytes, decoded.error());
        }

        // The object path: recover the concrete T from the carrier's slot (the demux has
        // already native-key-matched) and invoke the SAME callback for its duration.
        void on_object(const io::object_carrier &carrier, const io::message_info &info)
        {
            callback(*static_cast<const value_type *>(carrier.slot->object), info);
        }
    };

    std::unique_ptr<state>                     m_state;
    plexus::detail::move_only_function<void()> m_retire;
};

}

#endif
