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
#include "plexus/detail/subscriber_callback.h"

#include <span>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <functional>
#include <type_traits>
#include <string_view>
#include <system_error>

namespace plexus {

template<typename Codec>
class subscriber;

// posture is the typed attach gate (lenient attaches to an untyped producer; strict refuses).
// on_decode_failure absent is drop+count+warn, present hands the raw bytes + errc. type_id
// overrides the identity of a codec that carries one, absent uses the codec's own; capture absent
// falls back to the node-level recording_qos.
struct typed_subscriber_options
{
    io::subscriber_qos qos{};
    io::attach_posture posture = io::attach_posture::lenient;
    std::optional<type_identity> type_id{};
    std::optional<std::function<void(std::span<const std::byte>, std::error_code)>> on_decode_failure{};
    std::optional<recording_qos> capture{};
};

// LIFETIME: a subscriber must NOT outlive its node (member-init aggregation, node ref first).
// Dropping the handle retires the demand and, when it was the last local subscriber for the
// fqn, unsubscribes the topic from every fanned peer.
template<>
class subscriber<void>
{
public:
    template<typename Policy, typename... NodeTs, typename Cb>
    subscriber(node<Policy, NodeTs...> &n, std::string_view fqn, Cb cb)
            : subscriber(n, fqn, io::subscriber_qos{}, std::move(cb))
    {
    }

    template<typename Policy, typename... NodeTs, typename Cb>
    subscriber(node<Policy, NodeTs...> &n, std::string_view fqn, const io::subscriber_qos &qos, Cb cb)
    {
        // wants_message_info is carried on the LOCAL qos only, never the subscribe wire region.
        io::subscriber_qos local_qos = qos;
        local_qos.wants_message_info = std::is_invocable_v<Cb &, std::span<const std::byte>, const io::message_info &>;
        io::endpoint_seam seam       = n.endpoint_seam_for();
        const auto rid =
                seam.register_subscriber(seam.ctx, fqn, local_qos, plexus::detail::adapt_bytes_callback(std::move(cb)), std::nullopt, {}, nullptr, io::object_dispatch{}, std::nullopt);
        m_retire = [seam, rid] { seam.retire_subscriber(seam.ctx, rid); };
    }

    subscriber(subscriber &&other) noexcept
            : m_retire(std::exchange(other.m_retire, nullptr))
    {
    }

    subscriber(const subscriber &)            = delete;
    subscriber &operator=(const subscriber &) = delete;
    subscriber &operator=(subscriber &&)      = delete;

    ~subscriber()
    {
        if(m_retire != nullptr)
            m_retire();
    }

private:
    plexus::detail::move_only_function<void()> m_retire;
};

// The ctor registers BOTH a bytes decode adapter and a process-tier object-dispatch entry
// (native-key-checked) under ONE registration id, so retire is atomic.
//
// VIEW-T LIFETIME: a view-type value_type (aliasing the decode span / carrier slot) is valid
// for the callback invocation ONLY; deferred consumption must copy out before the return.
//
// LIFETIME: a subscriber must NOT outlive its node. The decode state lives in a heap block the
// handle owns; the node's adapters reference it by raw pointer and are retired before the block
// is freed.
template<typename Codec>
class subscriber
{
public:
    using value_type     = typename Codec::value_type;
    using typed_callback = plexus::detail::move_only_function<void(const value_type &, const io::message_info &)>;
    using state          = plexus::detail::subscriber_state<Codec, value_type, typed_callback>;

    static_assert(typed_codec<Codec>,
                  "plexus: a typed subscriber needs a codec satisfying typed_codec "
                  "(value_type; encode(const value_type&) -> wire_bytes<>; "
                  "decode(span, value_type&) -> expected<void, error_code>).");
    static_assert(identity_bearing<Codec>,
                  "plexus: a typed endpoint needs a type identity — give the subscriber's codec a "
                  "type_info() -> type_identity member, or wrap it in plexus::with_identity.");

    template<typename Policy, typename... NodeTs, typename Cb>
    subscriber(node<Policy, NodeTs...> &n, std::string_view fqn, Cb cb)
            : subscriber(n, fqn, typed_subscriber_options{}, std::move(cb), Codec{})
    {
    }

    template<typename Policy, typename... NodeTs, typename Cb>
    subscriber(node<Policy, NodeTs...> &n, std::string_view fqn, const typed_subscriber_options &opts, Cb cb, Codec codec = {})
            : m_seam(n.endpoint_seam_for())
            , m_state(std::make_unique<state>(std::move(codec), plexus::detail::adapt_typed_callback<value_type>(std::move(cb)), opts.on_decode_failure))
            , m_rid(enroll<Cb>(m_seam, fqn, opts, *m_state))
    {
    }

    // Inbound frames whose decode failed — dropped, never a partial T.
    std::size_t decode_failed() const noexcept
    {
        m_seam.affine(m_seam.ctx, "decode_failed");
        return m_state->decode_failed;
    }

    subscriber(subscriber &&other) noexcept
            : m_seam(std::exchange(other.m_seam, io::endpoint_seam{}))
            , m_state(std::move(other.m_state))
            , m_rid(other.m_rid)
    {
    }

    subscriber(const subscriber &)            = delete;
    subscriber &operator=(const subscriber &) = delete;
    subscriber &operator=(subscriber &&)      = delete;

    ~subscriber()
    {
        if(m_seam.ctx != nullptr)
            m_seam.retire_subscriber(m_seam.ctx, m_rid);
    }

private:
    io::endpoint_seam m_seam;
    std::unique_ptr<state> m_state;
    std::uint64_t m_rid;

    template<typename Cb>
    static std::uint64_t enroll(const io::endpoint_seam &seam, std::string_view fqn, const typed_subscriber_options &opts, state &st)
    {
        const auto identity    = resolve_identity(st.codec, opts.type_id);
        io::subscriber_qos qos = opts.qos;
        qos.posture            = opts.posture;
        // wants_message_info is carried on the LOCAL qos only, never the subscribe wire region.
        qos.wants_message_info = std::is_invocable_v<Cb &, const value_type &, const io::message_info &> && opts.qos.wants_message_info;
        return plexus::detail::register_typed(seam, fqn, qos, &st, identity, &io::detail::type_key<value_type>, opts.capture ? std::optional{opts.capture->to_rule()} : std::nullopt);
    }
};

}

#endif
