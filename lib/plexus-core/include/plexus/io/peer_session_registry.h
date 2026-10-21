#ifndef HPP_GUARD_PLEXUS_IO_PEER_SESSION_REGISTRY_H
#define HPP_GUARD_PLEXUS_IO_PEER_SESSION_REGISTRY_H

#include "plexus/policy.h"
#include "plexus/node_id.h"

#include "plexus/detail/compat.h"

#include "plexus/io/node_name.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/reconnect.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/peer_session.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/session_build_context.h"

#include "plexus/io/detail/peer_session_slot.h"
#include "plexus/io/detail/peer_session_build.h"
#include "plexus/io/detail/peer_session_rebuild.h"
#include "plexus/io/detail/peer_session_retire.h"

#include "plexus/log/logger.h"

#include <map>
#include <chrono>
#include <memory>
#include <string>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::io {

template<typename Policy, typename Transport, typename Clock = std::chrono::steady_clock>
    requires plexus::Policy<Policy> && transport_backend<Transport, Policy>
class peer_session_registry
{
public:
    using session_type = peer_session<Policy>;
    using driver_type  = reconnect<Policy, Transport, Clock>;
    using channel_type = typename Policy::byte_channel_type;

    peer_session_registry(Transport &transport, session_build_context<Policy> &build)
            : m_transport(transport)
            , m_next_inbound(1)
            , m_build(build)
    {
    }

    // Returns whether a slot for id exists after the call: false when the endpoint is already claimed
    // by another peer (no slot is created), so callers must not reach for the driver in that case.
    bool ensure_slot(const node_id &id, const endpoint &ep, const std::string &node_name)
    {
        if(m_slots.find(id) != m_slots.end())
            return true;
        if(detail::endpoint_claimed(*this, id, ep))
        {
            m_build.logger.warn("plexus: dial endpoint already claimed by another peer — slot not created");
            return false;
        }
        auto slot = std::make_unique<slot_block>(m_transport, m_build, id, ep, node_name);
        detail::wire_redial(*this, *slot, id);
        detail::wire_dead(*this, *slot, id);
        m_slots.emplace(id, std::move(slot));
        return true;
    }

    void build_session_for_endpoint(const endpoint &ep, std::unique_ptr<channel_type> channel)
    {
        detail::build_for_endpoint(*this, ep, std::move(channel));
    }

    // Awareness for id was withdrawn: cancel an armed backoff and refuse a redial for its slot, so
    // a forget cannot be undone by a reconnect the peer's own teardown had already armed.
    void deny_redial(const node_id &id)
    {
        auto it = m_slots.find(id);
        if(it != m_slots.end())
            it->second->driver.deny_redial();
    }

    template<typename PreBuild = std::nullptr_t>
    node_id accept_session(std::unique_ptr<channel_type> channel, PreBuild &&pre_build = nullptr)
    {
        return detail::accept_into_slot(*this, std::move(channel), std::forward<PreBuild>(pre_build));
    }

    std::optional<node_id> id_for_endpoint(const endpoint &ep) const
    {
        for(const auto &[id, slot] : m_slots)
            if(slot->record.dial_endpoint == ep)
                return id;
        return std::nullopt;
    }

    bool has_session(const node_id &id) const
    {
        auto it = m_slots.find(id);
        return it != m_slots.end() && it->second->session.has_value();
    }

    bool is_connected(const node_id &id) const
    {
        auto it = m_slots.find(id);
        return it != m_slots.end() && it->second->session.has_value() && it->second->session->is_complete();
    }

    session_type *session_for(const node_id &id)
    {
        auto it = m_slots.find(id);
        if(it == m_slots.end() || !it->second->session.has_value())
            return nullptr;
        return &*it->second->session;
    }

    session_type *session_for_name(std::string_view node_name)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->record.node_name == node_name)
                return slot->session.has_value() ? &*slot->session : nullptr;
        return nullptr;
    }

    bool same_host_for(std::string_view node_name) const
    {
        for(const auto &[id, slot] : m_slots)
            if(slot->record.node_name == node_name)
                return slot->session.has_value() && slot->session->same_host();
        return false;
    }

    template<typename Fn>
    void for_each_connected(Fn &&fn)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->session && slot->session->is_complete())
                fn(id, *slot->session);
    }

    driver_type &driver_for(const node_id &id)
    {
        return m_slots.at(id)->driver;
    }

    std::uint32_t attempt_count(const node_id &id) const
    {
        return m_slots.at(id)->driver.attempt_count();
    }

    bool is_dead(const node_id &id) const
    {
        auto it = m_slots.find(id);
        return it != m_slots.end() && it->second->dead;
    }

    void notify_dial_failed(const endpoint &ep)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->record.dial_endpoint == ep)
                return slot->driver.notify_dial_failed();
    }

    void close(detail::pending_ops &owner)
    {
        detail::close_slots(*this, owner);
    }

    void hang_up(const node_id &id)
    {
        detail::hang_up_slot(*this, id);
    }

private:
    using slot_block = detail::session_slot<Policy, Transport, Clock>;

    Transport &m_transport;
    std::uint64_t m_next_inbound;
    session_build_context<Policy> &m_build;
    std::map<node_id, std::unique_ptr<slot_block>> m_slots;

    template<typename R, typename S>
    friend void detail::wire_drop(R &, S &, const node_id &);
    template<typename R, typename S>
    friend void detail::wire_inbound_drop(R &, S &, const node_id &);
    template<typename R, typename S>
    friend void detail::wire_observer(R &, S &);
    template<typename R, typename S>
    friend void detail::wire_message_route(R &, S &);
    template<typename R, typename S>
    friend void detail::wire_object_route(R &, S &);
    template<typename R, typename S>
    friend void detail::wire_security(R &, S &);
    template<typename R, typename S>
    friend void detail::wire_redial(R &, S &, const node_id &);
    template<typename R, typename S>
    friend void detail::wire_dead(R &, S &, const node_id &);
    template<typename R, typename S>
    friend void detail::fire_dead(R &, S &, const node_id &);
    template<typename R, typename S>
    friend void detail::build_session_into(R &, S &, std::unique_ptr<typename R::channel_type>, bool);
    template<typename R>
    friend bool detail::endpoint_claimed(const R &, const node_id &, const endpoint &);
    template<typename R>
    friend void detail::build_for_endpoint(R &, const endpoint &, std::unique_ptr<typename R::channel_type>);
    template<typename R>
    friend typename R::slot_block &detail::emplace_inbound_slot(R &);
    template<typename R>
    friend void detail::retire_unclaimed(R &, std::unique_ptr<typename R::channel_type>);
    template<typename R>
    friend void detail::close_slots(R &, detail::pending_ops &);
    template<typename R>
    friend void detail::hang_up_slot(R &, const node_id &);
};

}

#endif
