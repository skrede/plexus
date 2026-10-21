#ifndef HPP_GUARD_PLEXUS_SHM_SHM_BYTE_CHANNEL_H
#define HPP_GUARD_PLEXUS_SHM_SHM_BYTE_CHANNEL_H

#include "plexus/shm/shm_channel.h"
#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"
#include "plexus/shm/region_broker_concept.h"
#include "plexus/shm/shm_companion_consumer.h"

#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/scheduler_key.h"
#include "plexus/io/detail/close_notification.h"
#include "plexus/wire/close_cause.h"
#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::shm {

// Hands one closure to the executor the channels of a mux member run on.
using post_fn = plexus::detail::move_only_function<void(plexus::detail::move_only_function<void()>)>;

// The per-topic byte_channel the shm mux member hands up, wrapping ONE live ring behind the
// byte_channel verbs the multiplexer erases. close() releases the role it acquired (the last
// release tears the ring down) and owns no outstanding operation, so on_closed is posted at once.
// on_protocol_close never fires (no byte-stream framing on a ring). The registry and the post
// outlive every channel minted over them. When the ring's waiter is lost, lost runs, or with none
// the channel closes as any byte_channel whose medium died.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_byte_channel
{
public:
    using registry_type = shm_topic_registry<Broker, Notifier>;

    shm_byte_channel(registry_type &registry, post_fn &post, shm_channel<Notifier> &channel, std::string fqn, const node_id &publisher, io::endpoint remote, ring_role role,
                     waiter_loss::lost_fn lost = nullptr)
            : m_registry(registry)
            , m_post(post)
            , m_channel(channel)
            , m_publisher(publisher)
            , m_fqn(std::move(fqn))
            , m_remote(std::move(remote))
            , m_role(role)
            , m_loss(registry.watch_loss(m_fqn, ring_direction::request, m_publisher, lost ? std::move(lost) : waiter_loss::lost_fn{[this] { close(); }}))
            , m_scheduler_key(io::detail::next_scheduler_key())
            , m_released(false)
    {
    }

    shm_byte_channel(const shm_byte_channel &)            = delete;
    shm_byte_channel &operator=(const shm_byte_channel &) = delete;

    ~shm_byte_channel()
    {
        if(!m_released)
            m_registry.release(m_fqn, ring_direction::request, m_publisher, m_role);
    }

    // An oversize payload surfaces message_too_large, a stalled reliable path would_block; the
    // channel stays open either way so the publisher learns the send failed rather than a silent drop.
    void send(std::span<const std::byte> data)
    {
        if(m_released)
            return;
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

    // Idempotent: a second close is a no-op.
    void close()
    {
        if(m_released)
            return;
        m_released          = true;
        m_on_data           = nullptr;
        m_on_error          = nullptr;
        m_on_drop           = nullptr;
        m_on_protocol_close = nullptr;
        m_registry.release(m_fqn, ring_direction::request, m_publisher, m_role);
        notify_closed();
    }

    io::endpoint remote_endpoint() const
    {
        return m_remote;
    }

    // Install the data sink and pump whatever is already pending.
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
        pump();
    }

    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_close_notification.install(std::move(cb));
        if(m_released)
            notify_closed();
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

    // send() memcpys straight into the slab with no bounded egress queue, so this channel keeps
    // no backlog. Reports 0 ("always accepts").
    std::size_t backpressured() const noexcept
    {
        return 0;
    }

    // Distinct per construction so a reconnect cannot alias a freed member's entry.
    std::uint64_t scheduler_key() const noexcept
    {
        return m_scheduler_key;
    }

    // Driven by the owner on each notifier wake (the registry's own drain discards; this delivers).
    void pump()
    {
        if(m_released || !m_on_data)
            return;
        typename shm_channel<Notifier>::deliver_fn deliver = [this](::plexus::wire_bytes<shm_slot_owner> wb)
        {
            if(m_on_data)
                m_on_data(std::span<const std::byte>{wb.data(), wb.size()});
        };
        m_channel.drain(deliver);
    }

private:
    void notify_closed()
    {
        m_close_notification.notify([this](plexus::detail::move_only_function<void()> notify) { m_post(std::move(notify)); });
    }

    // The engine binds m_on_drop to its posted drop_sink, so the emit reaches the fan-out POSTED.
    void emit_drop()
    {
        if(m_on_drop)
            m_on_drop(io::detail::drop_event{.cause = io::detail::drop_cause::blocked, .transport = io::locality::local});
    }

    registry_type &m_registry;
    post_fn &m_post;
    shm_channel<Notifier> &m_channel;
    node_id m_publisher;
    std::string m_fqn;
    io::endpoint m_remote;
    ring_role m_role;
    loss_watch m_loss;
    std::uint64_t m_scheduler_key;
    bool m_released;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    io::detail::close_notification m_close_notification;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(const io::detail::drop_event &)> m_on_drop;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
};

}

#endif
