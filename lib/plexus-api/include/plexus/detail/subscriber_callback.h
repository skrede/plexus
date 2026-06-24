#ifndef HPP_GUARD_PLEXUS_DETAIL_SUBSCRIBER_CALLBACK_H
#define HPP_GUARD_PLEXUS_DETAIL_SUBSCRIBER_CALLBACK_H

#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/endpoint_seam.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <functional>
#include <type_traits>
#include <string_view>
#include <system_error>

namespace plexus::detail {

// Normalize a user bytes callback to the node's 3-arg demux shape, the arity resolved ONCE here
// (the cold path), so the hot demux fans a uniform signature with no per-frame branch.
template<typename Cb>
plexus::detail::move_only_function<void(std::span<const std::byte>, const io::message_info &)> adapt_bytes_callback(Cb cb)
{
    if constexpr(std::is_invocable_v<Cb &, std::span<const std::byte>, const io::message_info &>)
        return [cb = std::move(cb)](std::span<const std::byte> bytes, const io::message_info &info) mutable { cb(bytes, info); };
    else
        return [cb = std::move(cb)](std::span<const std::byte> bytes, const io::message_info &) mutable { cb(bytes); };
}

// Normalize a user typed callback to the 2-arg typed shape, the arity resolved ONCE here.
template<typename ValueType, typename Cb>
plexus::detail::move_only_function<void(const ValueType &, const io::message_info &)> adapt_typed_callback(Cb cb)
{
    if constexpr(std::is_invocable_v<Cb &, const ValueType &, const io::message_info &>)
        return [cb = std::move(cb)](const ValueType &v, const io::message_info &info) mutable { cb(v, info); };
    else
        return [cb = std::move(cb)](const ValueType &v, const io::message_info &) mutable { cb(v); };
}

// The heap-stable decode state the node's adapters reference by raw pointer. The reused slot is
// per-handle, so there is no per-message allocation. value_type / typed_callback are the typed
// subscriber's; T is the codec.
template<typename Codec, typename ValueType, typename TypedCallback>
struct subscriber_state
{
    Codec                                                                           codec;
    TypedCallback                                                                   callback;
    std::optional<std::function<void(std::span<const std::byte>, std::error_code)>> on_failure;
    ValueType                                                                       slot{};
    std::size_t                                                                     decode_failed{};

    subscriber_state(Codec c, TypedCallback cb, std::optional<std::function<void(std::span<const std::byte>, std::error_code)>> fail)
            : codec(std::move(c))
            , callback(std::move(cb))
            , on_failure(std::move(fail))
    {
    }

    // Decode into the reused slot; failure drops — NEVER a partial T, never a teardown.
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

    // The demux has already native-key-matched: recover the concrete T from the carrier.
    void on_object(const io::object_carrier &carrier, const io::message_info &info)
    {
        callback(*static_cast<const ValueType *>(carrier.slot->object), info);
    }
};

// Install ONE registration carrying BOTH a bytes decode adapter and a process-tier object-dispatch
// entry bound to the heap-stable state, so retire is atomic. Returns the registration id. The
// state outlives the adapters (the handle retires them before freeing the block).
template<typename State>
std::uint64_t register_typed(const io::endpoint_seam &seam, std::string_view fqn, const io::subscriber_qos &qos, State *st, std::optional<std::uint64_t> type_id, const void *native_key,
                             std::optional<io::topic_capture_rule> capture)
{
    auto                bytes_adapter = [st](std::span<const std::byte> bytes, const io::message_info &info) { st->on_bytes(bytes, info); };
    io::object_dispatch dispatch      = [st](const io::object_carrier &carrier, const io::message_info &info) { st->on_object(carrier, info); };
    return seam.register_subscriber(seam.ctx, fqn, qos, std::move(bytes_adapter), type_id, native_key, std::move(dispatch), capture);
}

}

#endif
