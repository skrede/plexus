#ifndef HPP_GUARD_PLEXUS_SHM_SHM_BYTE_CHANNEL_H
#define HPP_GUARD_PLEXUS_SHM_SHM_BYTE_CHANNEL_H

#include "plexus/shm/region_broker_concept.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/scheduler_key.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::shm {

// The per-topic byte_channel the shm mux member hands up: wraps ONE live ring behind the
// byte_channel verbs the multiplexer erases. send memcpy-publishes into the slab and wakes the
// cross-process consumer; the notifier-driven drain pumps received messages into on_data as
// header-on bytes. close() releases the ring (refcount tears it down + unlinks at 1->0).
// on_protocol_close never fires (no byte-stream framing on a ring). Borrows the registry +
// channel BY REFERENCE; the registry outlives every channel it mints.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_byte_channel
{
public:
    using registry_type = shm_topic_registry<Broker, Notifier>;

    shm_byte_channel(registry_type &registry, shm_channel<Notifier> &channel, std::string fqn, io::endpoint remote) noexcept
            : m_registry(registry)
            , m_channel(channel)
            , m_fqn(std::move(fqn))
            , m_remote(std::move(remote))
    {
    }

    shm_byte_channel(const shm_byte_channel &)            = delete;
    shm_byte_channel &operator=(const shm_byte_channel &) = delete;

    ~shm_byte_channel()
    {
        if(!m_released)
            m_registry.release(m_fqn, ring_direction::request);
    }

    // memcpy into the slab -> publish -> signal. An oversize payload surfaces message_too_large;
    // a stalled reliable path surfaces would_block. The channel stays open either way (the bytes
    // never left the node) — the publisher learns the send failed rather than a silent drop.
    void send(std::span<const std::byte> data)
    {
        const loan_status st = m_channel.send(data);
        if(st == loan_status::rejected)
        {
            if(m_on_error)
                m_on_error(io::io_error::message_too_large);
            emit_drop();
        }
        else if(st == loan_status::congested)
        {
            if(m_on_error)
                m_on_error(io::io_error::would_block);
            emit_drop();
        }
    }

    // Release the ring back to the registry (idempotent: a second close is a no-op).
    void close()
    {
        if(m_released)
            return;
        m_released = true;
        m_registry.release(m_fqn, ring_direction::request);
        if(m_on_closed)
            m_on_closed();
    }

    [[nodiscard]] io::endpoint remote_endpoint() const
    {
        return m_remote;
    }

    // Install the data sink and pump whatever is already pending: this channel routes drained
    // bytes to ITS sink, re-draining here and on each pump() the owner drives off a wake.
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
        pump();
    }

    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_on_closed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }
    void on_drop(plexus::detail::move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close = std::move(cb);
    }

    // send() memcpys straight into the shared-memory slab (no bounded userspace egress queue):
    // the ring's own io::congestion verdict is surfaced inline at send time, so this channel
    // keeps no queued backlog. Reports 0 ("always accepts") — the in-slab fire-through signal.
    [[nodiscard]] std::size_t backpressured() const noexcept
    {
        return 0;
    }

    // The stable per-construction egress key (distinct so a reconnect cannot alias a freed
    // member's entry); a mux-composed shm member keys the scheduler band map as a stream member.
    [[nodiscard]] std::uint64_t scheduler_key() const noexcept
    {
        return m_scheduler_key;
    }

    // Drain every pending message into on_data as header-on bytes, driven by the owner on each
    // notifier wake (the registry's own drain discards; this delivers).
    void pump()
    {
        if(!m_on_data)
            return;
        typename shm_channel<Notifier>::deliver_fn deliver = [this](::plexus::wire_bytes<shm_slot_owner> wb) { m_on_data(std::span<const std::byte>{wb.data(), wb.size()}); };
        m_channel.drain(deliver);
    }

private:
    // The drop edge is reported straight off send() as drop_cause::blocked. The engine binds
    // m_on_drop to its posted drop_sink, so the emit reaches the fan-out POSTED.
    void emit_drop()
    {
        if(m_on_drop)
            m_on_drop(io::detail::drop_event{.cause = io::detail::drop_cause::blocked, .transport = io::locality::local});
    }

    registry_type                                                           &m_registry;
    shm_channel<Notifier>                                                   &m_channel;
    std::string                                                              m_fqn;
    io::endpoint                                                             m_remote;
    std::uint64_t                                                            m_scheduler_key{io::detail::next_scheduler_key()};
    bool                                                                     m_released = false;
    plexus::detail::move_only_function<void(std::span<const std::byte>)>     m_on_data;
    plexus::detail::move_only_function<void()>                               m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)>                   m_on_error;
    plexus::detail::move_only_function<void(const io::detail::drop_event &)> m_on_drop;
    plexus::detail::move_only_function<void(wire::close_cause)>              m_on_protocol_close;
};

// The RAII receive lane over a co-host companion ring: holds ONE request-direction ring
// refcount, drained by the registry's armed notifier (posted on the node executor — NO drain
// thread). Destruction clears the sink BEFORE dropping the refcount (disarm-before-destroy), so
// a wake in flight never delivers onto a torn-down sink and a release at 1->0 unmaps the ring.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_companion_consumer
{
public:
    using registry_type = shm_topic_registry<Broker, Notifier>;

    shm_companion_consumer(registry_type &registry, std::string fqn) noexcept
            : m_registry(registry)
            , m_fqn(std::move(fqn))
    {
    }

    shm_companion_consumer(const shm_companion_consumer &)            = delete;
    shm_companion_consumer &operator=(const shm_companion_consumer &) = delete;

    ~shm_companion_consumer()
    {
        m_registry.clear_consumer_sink(m_fqn, ring_direction::request);
        m_registry.release(m_fqn, ring_direction::request);
    }

private:
    registry_type &m_registry;
    std::string    m_fqn;
};

}

#endif
