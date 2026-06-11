#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_CHANNEL_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_CHANNEL_H

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include <span>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::inproc {

// In-process byte_channel: a per-connection byte stream over the inproc_bus.
// The channel registers on the bus at construction and is assigned a local
// endpoint; connect_to() names the partner it sends toward. send() enqueues the
// bytes to the partner via the bus and returns immediately — the partner's
// on_data does not fire until the executor's step-loop delivers the packet, so a
// handler can call back into the channel with no re-entrancy hazard. close() posts
// a broken-pipe error to the partner through the same step-loop. Satisfies
// plexus::io::byte_channel.
template <typename Clock = std::chrono::steady_clock>
class inproc_channel
{
public:
    explicit inproc_channel(inproc_executor<Clock> &ex)
        : m_exec(&ex)
        , m_bus(&ex.bus())
        , m_local(m_bus->register_channel(this))
    {
    }

    inproc_channel(inproc_executor<Clock> &ex, std::error_code &)
        : inproc_channel(ex)
    {
    }

    ~inproc_channel()
    {
        if(m_bus)
            m_bus->deregister_channel(this);
    }

    inproc_channel(const inproc_channel &) = delete;
    inproc_channel &operator=(const inproc_channel &) = delete;
    inproc_channel(inproc_channel &&) = delete;
    inproc_channel &operator=(inproc_channel &&) = delete;

    // Name the partner this channel sends toward. Pairing is a post-construction
    // step because the Policy requires construction from the executor alone. The
    // partner's bus key is resolved HERE, once: bus keys are never reused, so the
    // hot send path addresses packets by integer key (the endpoint strings stay
    // cold), and a partner endpoint the bus never minted resolves to 0 — its sends
    // drop in deliver_one exactly as an unmatched endpoint did.
    void connect_to(const io::endpoint &partner)
    {
        m_partner = partner;
        m_partner_key = m_bus ? m_bus->key_for(partner) : 0;
    }

    [[nodiscard]] const io::endpoint &local_endpoint() const noexcept { return m_local; }

    void send(std::span<const std::byte> data)
    {
        if(m_bus && !m_closed)
            m_bus->enqueue(m_partner_key, data);
    }

    // The process-tier object lane mirroring send(): enqueue a refcounted object
    // handle to the partner through the same bus FIFO. A send on a closed channel
    // does NOT enqueue and therefore does NOT addref, so no release is owed for it.
    void send_object(const io::object_carrier &carrier)
    {
        if(m_bus && !m_closed)
            m_bus->enqueue_object(m_partner_key, carrier);
    }

    void close()
    {
        if(m_closed)
            return;
        m_closed = true;
        // Notify the partner through the step-loop, never synchronously: the
        // close is queued like a packet and surfaces on the partner only from
        // inproc_bus::deliver_one() inside step().
        if(m_bus)
            m_bus->enqueue_close(m_partner_key);
    }

    [[nodiscard]] io::endpoint remote_endpoint() const { return m_partner; }

    void on_data(detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_object(detail::move_only_function<void(const io::object_carrier &)> cb) { m_on_object = std::move(cb); }
    void on_closed(detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    // The non-stream opt-out: inproc moves whole pre-framed packets, so no partial
    // frame — and thus no framing violation or slowloris stall — can ever exist on
    // it. The callback is stored to satisfy the uniform channel seam and is NEVER
    // fired (a protocol-close is meaningless without a byte stream to misbehave on).
    void on_protocol_close(detail::move_only_function<void(wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    // Bus callbacks: invoked only from inproc_bus::deliver_one(), i.e. from
    // inside the executor's step(). Never reached synchronously from a peer's
    // send()/close().
    void deliver(std::span<const std::byte> data)
    {
        if(!m_closed && m_on_data)
            m_on_data(data);
    }

    // Hand a delivered object to the stored callback, transferring the bus's reference
    // TO the callback (which owns the matching release). A delivery to a closed or
    // handlerless channel has no callback to take the reference, so it releases here —
    // the reference the bus handed over is balanced on every path, so no slot leaks.
    void deliver_object(const io::object_carrier &carrier)
    {
        if(!m_closed && m_on_object)
            m_on_object(carrier);
        else
            io::release(carrier);
    }

    void deliver_close()
    {
        if(m_closed)
            return;
        if(m_on_closed)
            m_on_closed();
        if(m_on_error)
            m_on_error(io::io_error::broken_pipe);
    }

    // Surface a wire-protocol close (a framing violation or, once an AEAD decorator wraps
    // the link, a tag-verify failure) — the on_protocol_close seam a real byte-stream
    // channel fires, exposed here so the deterministic rig drives the misbehavior path
    // the bus cannot otherwise produce.
    void deliver_protocol_close(wire::close_cause cause)
    {
        if(m_on_protocol_close)
            m_on_protocol_close(cause);
    }

private:
    inproc_executor<Clock> *m_exec;
    inproc_bus<Clock> *m_bus;
    io::endpoint m_local;
    io::endpoint m_partner;
    std::uint64_t m_partner_key{0};
    detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    detail::move_only_function<void(const io::object_carrier &)> m_on_object;
    detail::move_only_function<void()> m_on_closed;
    detail::move_only_function<void(io::io_error)> m_on_error;
    detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
    bool m_closed{false};
};

}

#endif
