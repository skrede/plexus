#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_CHANNEL_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_CHANNEL_H

#include "plexus/io/byte_channel.h"
#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/io/recording/wire_record.h"

#include "plexus/detail/compat.h"

#include <span>
#include <memory>
#include <utility>
#include <cstddef>
#include <cstdint>

namespace plexus::io {

// A LOSSLESS byte_channel decorator: the authenticated_channel structural precedent
// (wrap a Lower, intercept send(), re-emit on_data, delete copy/move, forward the seven
// verbs, static_assert byte_channel) but it NEVER transforms — it taps the full framed
// bytes on both directions and forwards them VERBATIM. send() taps the OUT frame then
// hands the unchanged bytes to the lower channel; the lower channel's on_data taps the IN
// frame then re-emits the SAME bytes upward. The captured bytes are byte-identical to what
// the lower channel sent or received.
//
// Unlike the authenticated_channel precedent (which BORROWS a Lower &), this OWNS its Lower
// via unique_ptr: the channel-mint point moves a unique_ptr<channel_type>, so the decorator
// takes ownership at construction (the channel_adapter-style wrap, not the borrow).
//
// The tap is a null-default on_wire edge — the decorator is recorder-agnostic. The engine
// installs a posted sink into it at the single channel-mint point (the wire_channel_drop
// precedent), so a captured frame crosses to the recorder POSTED on the executor, never a
// synchronous io-thread push into the single-writer ring. With no tap installed the edge is
// a single null-check branch per frame and never fires.
template<typename Lower>
class recording_channel
{
public:
    using wire_tap = plexus::detail::move_only_function<void(recording::wire_direction, std::uint64_t, std::span<const std::byte>)>;

    explicit recording_channel(std::unique_ptr<Lower> lower)
            : m_lower(std::move(lower))
    {
        wire_lower();
    }

    recording_channel(const recording_channel &)            = delete;
    recording_channel &operator=(const recording_channel &) = delete;
    recording_channel(recording_channel &&)                 = delete;
    recording_channel &operator=(recording_channel &&)      = delete;

    // The per-direction sequence is decorator-LOCAL: a strictly monotonic counter the
    // capture stamps on every frame regardless of transport. It is the transport-uniform,
    // clock-skew-immune cross-node loss-join key (a frame present in one node's out run and
    // absent from the peer's in run is a structural drop, found by sequence arithmetic over
    // the two recorded runs with no reference to either node's clock). The datagram AEAD seq
    // and the udp_envelope seq carry the same information at the transport layer, but the
    // cleartext stream wire has no per-direction publication sequence, so this counter is the
    // single key spanning every transport — never a new field on the actual wire.
    void send(std::span<const std::byte> data)
    {
        if(m_on_wire)
            m_on_wire(recording::wire_direction::out, m_send_seq++, data);
        m_lower->send(data);
    }

    void close()
    {
        m_lower->close();
    }

    endpoint remote_endpoint() const
    {
        return m_lower->remote_endpoint();
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_lower->on_closed(std::move(cb));
    }
    void on_error(plexus::detail::move_only_function<void(io_error)> cb)
    {
        m_lower->on_error(std::move(cb));
    }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_lower->on_protocol_close(std::move(cb));
    }

    // The optional occupancy/scheduler/drop edges the erased multi-transport channel and the
    // egress scheduler reach for: forwarded only when the Lower has them, so the decorated
    // bare concrete channel still satisfies everything the registry calls without forcing an
    // edge onto a Lower that lacks one (the channel_adapter optional-edge discipline). The
    // egress scheduler's can_poll() gate reads backpressured() behind its own requires-clause,
    // so guarding it here keeps a decorated Lower that has no occupancy signal correctly off
    // the poll path rather than forcing the call onto a Lower that lacks it.
    std::size_t backpressured() const
        requires requires(const Lower &l) { l.backpressured(); }
    {
        return m_lower->backpressured();
    }
    std::uint64_t scheduler_key() const
        requires requires(const Lower &l) { l.scheduler_key(); }
    {
        return m_lower->scheduler_key();
    }

    void on_drop(plexus::detail::move_only_function<void(const detail::drop_event &)> cb)
        requires requires(Lower &l, plexus::detail::move_only_function<void(const detail::drop_event &)> c) { l.on_drop(std::move(c)); }
    {
        m_lower->on_drop(std::move(cb));
    }

    // The recorder-agnostic capture edge the engine installs a posted sink into. Null by
    // default — structural absence of a sink keeps the tap inert.
    void on_wire(wire_tap cb)
    {
        m_on_wire = std::move(cb);
    }

private:
    void wire_lower()
    {
        m_lower->on_data(
                [this](std::span<const std::byte> bytes)
                {
                    if(m_on_wire)
                        m_on_wire(recording::wire_direction::in, m_recv_seq++, bytes);
                    if(m_on_data)
                        m_on_data(bytes);
                });
    }

    std::unique_ptr<Lower> m_lower;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    wire_tap m_on_wire;
    std::uint64_t m_send_seq{0};
    std::uint64_t m_recv_seq{0};
};

// A compile-time witness of structural presence: false for any bare channel type, true
// only for a recording_channel specialization. A default (non-wire) node asserts its
// channel_type is NOT a recording_channel — the decorator is absent at compile time, not
// suppressed by a runtime branch.
template<typename T>
inline constexpr bool is_recording_channel_v = false;

template<typename Lower>
inline constexpr bool is_recording_channel_v<recording_channel<Lower>> = true;

}

// The lossless decorator over the erased multi-transport channel is itself a byte_channel:
// the seven-verb conformance is pinned so the decorated channel composes anywhere a bare one
// does.
static_assert(plexus::io::byte_channel<plexus::io::recording_channel<plexus::io::polymorphic_byte_channel>>, "recording_channel must satisfy byte_channel — check the seven verbs");

#endif
