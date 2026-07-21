#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARD_SPLICE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARD_SPLICE_H

#include "plexus/io/splice_pool.h"
#include "plexus/io/forward_options.h"

#include "plexus/wire_bytes.h"

#include "plexus/node_id.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/writer.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/forwarded_frame.h"

#include <span>
#include <limits>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::io {

// The non-relay twin: a members-less no-op whose refan does nothing, so a node not spelled relay<> pays
// zero for the forwarding-send path and instantiates no splice pool, no envelope encode, and no egress
// fan. It mirrors forward_splice's refan surface exactly, so one profile parameter threads both twins
// with no platform branch. (The null_peer_report_emitter precedent.)
struct null_forward_splice
{
    null_forward_splice() = default;

    explicit null_forward_splice(const forward_ctx &) noexcept
    {
    }

    template<typename Forwarder>
    void refan(Forwarder &, std::uint64_t, const node_id &, std::uint8_t, std::span<const std::byte>, const void *, const wire::shared_bytes *) noexcept
    {
    }

    std::uint64_t exhaustion_drops() const noexcept
    {
        return 0;
    }
};

namespace detail {

// The outbound leg's per-message envelope ceiling: a channel that publishes max_frame_bytes() (a
// real per-message size limit — a serial or datagram leg) gates the splice against it; a channel
// without the probe is UNLIMITED, so the gate is inert and a single-leg / wide-leg node stays
// byte-identical. The probe mirrors the can_poll()/scheduler_key() capability pattern.
template<typename Channel>
inline std::size_t channel_frame_ceiling(const Channel &ch) noexcept
{
    if constexpr(requires(const Channel &c) { c.max_frame_bytes(); })
        return ch.max_frame_bytes();
    else
        return (std::numeric_limits<std::size_t>::max)();
}

// The outbound forwarded frame put on the wire once and addref-shared to every destination: a fresh
// outer header (never the arrival frame's bytes, Pitfall 8) then the re-framed envelope. session_id 0
// on the data leg so the destination's staleness gate admits it regardless of its latched epoch. A
// pub/sub frame passes an unset destination (it transits by interest, the receive edge dedups and
// delivers by origin); a request/response frame passes the target identity so each hop re-resolves it.
inline std::size_t encode_forwarded_wire_into(std::span<std::byte> region, const node_id &origin, const node_id &destination, std::uint8_t hop, std::uint16_t seq,
                                              std::uint8_t flags, std::span<const std::byte> inner)
{
    const std::size_t inner_n = inner.size() < wire::detail::k_forwarded_inner_max ? inner.size() : wire::detail::k_forwarded_inner_max;
    const std::size_t payload  = wire::detail::forwarded_frame_preamble_size + sizeof(std::uint32_t) + inner_n;
    const wire::frame_header fhdr{.type = wire::msg_type::forwarded, .flags = 0, .session_id = 0, .timestamp_ns = wire::now_timestamp_ns(), .payload_len = payload};

    wire::writer w{region};
    w.bytes(wire::encode_header(fhdr));
    w.bytes(std::span<const std::byte>{origin.data(), origin.size()});
    w.bytes(std::span<const std::byte>{destination.data(), destination.size()});
    w.u8(hop);
    w.u16(seq);
    w.u8(flags);
    w.u32(static_cast<std::uint32_t>(inner_n));
    w.bytes(inner.first(inner_n));
    return w.offset();
}

}

// The relay-profile twin: the pub/sub forwarding-send half. It holds the grow-once splice pool and, per
// admitted forwarded frame, builds ONE outbound forwarded envelope (the D95.1 pooled owned copy by
// default; the owner-retaining zero-copy when the knob is refcounted_zero_copy and an inbound owner is
// available) and hands it to the forwarder's interest-scoped fan, which addref-shares it into every
// subscribing destination's band through the existing egress scheduler — no second queue.
template<typename Policy>
class forward_splice
{
public:
    using channel_type = typename Policy::byte_channel_type;

    explicit forward_splice(const forward_ctx &ctx)
            : m_pool(ctx.splice_pool_slots, ctx.splice_slot_bytes)
            , m_mode(ctx.ownership)
            , m_scope_all(ctx.scope_all)
            , m_scope(ctx.scope)
    {
    }

    // The forward-scope gate: which topics this relay is permitted to carry. An absent scope admits
    // every topic; a configured scope admits only topics its keyset intersects (the shipped matcher,
    // never a second glob); a misconfigured scope that failed to compile fails closed to none.
    bool forward_scope_admits(std::string_view fqn) const noexcept
    {
        if(m_scope_all)
            return true;
        if(!m_scope)
            return false;
        const auto key = match::key_pattern::make(fqn);
        return key.has_value() && m_scope->intersects(*key);
    }

    // Re-fan an admitted forwarded pub/sub inner frame to the topic's actual subscribers, excluding the
    // arrival session (the loop guard). The outbound envelope is framed once with hop+1 and a per-relay
    // seq, then shared to every destination band. A default (or owner-less) knob copies into a pool slot;
    // the opt-in knob with an inbound owner retains it with no copy. An exhausted pool yields an empty
    // buffer the fan degrades to drop-with-count — an empty owner is never enqueued.
    template<typename Forwarder>
    void refan(Forwarder &fwd, std::uint64_t hash, const node_id &origin, std::uint8_t hop, std::span<const std::byte> inner_frame, const channel_type *arrival,
               const wire::shared_bytes *owner)
    {
        const std::uint8_t out_hop = static_cast<std::uint8_t>(hop < 0xFF ? hop + 1 : hop);
        const std::uint16_t seq    = m_seq++;
        bool built = false;
        wire_bytes<> buf;
        auto build_once = [&]() -> const wire_bytes<> &
        {
            if(!built)
            {
                buf   = build(origin, out_hop, seq, inner_frame, owner);
                built = true;
            }
            return buf;
        };
        fwd.fan_forwarded_buffer(hash, static_cast<const void *>(arrival), build_once);
    }

    std::uint64_t exhaustion_drops() const noexcept
    {
        return m_pool.exhaustion_drops();
    }

private:
    wire_bytes<> build(const node_id &origin, std::uint8_t hop, std::uint16_t seq, std::span<const std::byte> inner, const wire::shared_bytes *owner)
    {
        if(m_mode == splice_ownership::refcounted_zero_copy && owner != nullptr && !owner->empty())
            return splice_pool::checkout_zero_copy(*owner);
        return m_pool.checkout_owned_copy([&](std::span<std::byte> slot)
                                          { return detail::encode_forwarded_wire_into(slot, origin, node_id{}, hop, seq, wire::k_forwarded_relay_consent_flag, inner); });
    }

    splice_pool m_pool;
    splice_ownership m_mode;
    bool m_scope_all;
    std::optional<match::key_pattern> m_scope;
    std::uint16_t m_seq{0};
};

}

#endif
