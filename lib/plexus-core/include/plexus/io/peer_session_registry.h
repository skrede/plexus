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

#include "plexus/io/detail/peer_session_build.h"

#include "plexus/log/logger.h"

#include <map>
#include <chrono>
#include <memory>
#include <string>
#include <cstdint>
#include <optional>
#include <type_traits>

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

    void ensure_slot(const node_id &id, const endpoint &ep, const std::string &node_name)
    {
        if(m_slots.find(id) != m_slots.end())
            return;
        if(endpoint_claimed(id, ep))
            return m_build.logger.warn("plexus: dial endpoint already claimed by another peer — slot not created");
        auto slot = std::make_unique<slot_block>(m_transport, m_build, id, ep, node_name);
        detail::wire_redial(*this, *slot, id);
        detail::wire_dead(*this, *slot, id);
        m_slots.emplace(id, std::move(slot));
    }

    void build_session_for_endpoint(const endpoint &ep, std::unique_ptr<channel_type> channel)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->record.dial_endpoint == ep)
            {
                slot->driver.mark_dial_settled();
                return build_into(*slot, std::move(channel), false);
            }
    }

    template<typename PreBuild = std::nullptr_t>
    node_id accept_session(std::unique_ptr<channel_type> channel, PreBuild &&pre_build = nullptr)
    {
        const node_id inbound_id = next_inbound_id();
        auto slot                = std::make_unique<slot_block>(m_transport, m_build, inbound_id, endpoint{}, node_name_of(inbound_id));
        auto [it, inserted]      = m_slots.emplace(inbound_id, std::move(slot));
        if(!inserted)
        {
            m_build.logger.warn("plexus: inbound id collision — accept dropped");
            return inbound_id;
        }
        if constexpr(!std::is_null_pointer_v<std::decay_t<PreBuild>>)
            pre_build(*channel, inbound_id);
        build_into(*it->second, std::move(channel), true);
        return inbound_id;
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

private:
    // The member order encodes the destruction discipline: session is destroyed first, so the
    // record (whose channel/epoch it borrows) and the driver outlive every incarnation.
    struct slot_block
    {
        slot_block(Transport &transport, session_build_context<Policy> &build, const node_id &id, const endpoint &ep, const std::string &node_name)
                : driver(transport, build.executor, build.redial, ep, build.redial_seed)
                , dead(false)
        {
            record.peer_id       = id;
            record.node_name     = node_name;
            record.dial_endpoint = ep;
        }

        peer_context<Policy> record;
        driver_type driver;
        std::optional<session_type> session;
        bool dead;
    };

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

    void build_into(slot_block &slot, std::unique_ptr<channel_type> channel, bool inbound)
    {
        slot.record.channel = std::move(channel);
        slot.session.reset();
        slot.session.emplace(slot.record, m_build.executor, m_build.fsm_cfg, m_build.handshake_timeout, m_build.messages, m_build.procedures, inbound, m_build.logger);
        if(inbound)
            detail::wire_inbound_drop(*this, slot, slot.record.peer_id);
        else
            detail::wire_drop(*this, slot, slot.record.peer_id);
        detail::wire_observer(*this, slot);
        detail::wire_message_route(*this, slot);
        detail::wire_object_route(*this, slot);
        detail::wire_security(*this, slot);
        slot.session->start();
    }

    node_id next_inbound_id()
    {
        const std::uint64_t n = m_next_inbound++;
        node_id id{};
        for(int i = 0; i < 8; ++i)
            id[15 - i] = std::byte{static_cast<std::uint8_t>(n >> (8 * i))};
        return id;
    }

    bool endpoint_claimed(const node_id &id, const endpoint &ep) const
    {
        for(const auto &[other, slot] : m_slots)
            if(other != id && slot->record.dial_endpoint == ep)
                return true;
        return false;
    }
};

}

#endif
