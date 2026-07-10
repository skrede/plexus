#ifndef HPP_GUARD_PLEXUS_DISCOVERY_MULTICAST_DISCOVERY_H
#define HPP_GUARD_PLEXUS_DISCOVERY_MULTICAST_DISCOVERY_H

#include "plexus/discovery/discovery_options.h"
#include "plexus/discovery/detail/announcement_card.h"
#include "plexus/discovery/detail/discovery_flood_cap.h"
#include "plexus/discovery/detail/discovery_card_codec.h"

#include "plexus/stream/datagram_socket.h"
#include "plexus/wire/announcement.h"
#include "plexus/discovery/discovery.h"
#include "plexus/detail/compat.h"

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <system_error>

namespace plexus::discovery {

// The first-party IPv4 multicast discovery leaf: it implements the abstract discovery::discovery
// (advertise/browse/stop) over a borrowed datagram_socket, so basic_node consumes it unchanged.
// advertise emits the node's card as a wire announcement immediately AND on a re-announce timer;
// browse decodes each inbound datagram, drops a foreign/malformed one (decode nullopt), and on a
// valid one fires on_resolved with metadata byte-identical to assemble_contact_card and the
// endpoint taken from the datagram's unspoofable kernel source. It posts awareness ONLY — the
// downstream handshake gates identity — so it never dials.
template<typename Socket, typename Policy, typename Clock = std::chrono::steady_clock>
    requires stream::datagram_socket<Socket>
class multicast_discovery final : public discovery
{
public:
    using executor_type = typename Policy::executor_type;

    multicast_discovery(executor_type executor, Socket &socket, discovery_options options = {})
            : m_executor(executor)
            , m_socket(socket)
            , m_timer(executor)
            , m_options(std::move(options))
            , m_flood_cap(m_options.cap)
            , m_advertising(false)
    {
        m_socket.bind(bind_endpoint());
        m_socket.on_datagram([this](const typename Socket::endpoint_type &from, std::span<const std::byte> bytes) { on_inbound(from, bytes); });
    }

    void advertise(const ::plexus::discovery::service_info &service) override
    {
        m_announcement             = detail::announcement_from_service_info(service, ttl_secs());
        const bool was_advertising = m_advertising;
        m_advertising              = true;
        emit_announcement();
        if(!was_advertising)
            arm_timer();
    }

    void browse(const resolved_callback &on_resolved) override
    {
        m_on_resolved_cb = on_resolved;
    }

    void on_withdrawn(const ::plexus::discovery::discovery::withdrawn_callback &cb) override
    {
        m_on_withdrawn_cb = cb;
    }

    void stop() override
    {
        m_timer.cancel();
        if(m_advertising)
            emit_goodbye();
        m_socket.close();
        m_on_resolved_cb  = nullptr;
        m_on_withdrawn_cb = nullptr;
        m_advertising     = false;
    }

private:
    typename Socket::endpoint_type bind_endpoint() const
    {
        typename Socket::endpoint_type ep{};
        ep.port(m_options.port);
        return ep;
    }

    void on_inbound(const typename Socket::endpoint_type &from, std::span<const std::byte> bytes)
    {
        const auto ann = wire::decode_announcement(bytes);
        if(!ann)
            return;
        const auto source = from.address().to_string();
        auto info         = detail::service_info_from_announcement(*ann, source);
        if((ann->flags & wire::k_announcement_goodbye_flag) != 0)
        {
            // A goodbye is a removal: never rate-limit or cap it, a leaver must always be able to leave.
            if(m_on_withdrawn_cb)
                m_on_withdrawn_cb(info);
            return;
        }
        if(m_on_resolved_cb && m_flood_cap.admit(source, Clock::now()))
            m_on_resolved_cb(info);
    }

    // Re-encode the cached announcement into the reused scratch (no per-emit alloc once warm) and
    // send. Re-announces of the same (id, ep) are idempotent at note_peer (a map put), so no dedup.
    void emit_with_flags(std::uint8_t flags)
    {
        if(!m_announcement)
            return;
        m_announcement->flags = flags;
        wire::encode_announcement_into(m_scratch, *m_announcement);
        m_socket.send_multicast(m_scratch);
    }

    void emit_announcement()
    {
        emit_with_flags(0);
    }

    // A best-effort single leave datagram: a browser evicts this node's awareness on receipt
    // instead of waiting out the ttl.
    void emit_goodbye()
    {
        emit_with_flags(wire::k_announcement_goodbye_flag);
    }

    void arm_timer()
    {
        m_timer.expires_after(m_options.announce_period);
        m_timer.async_wait(
                [this](std::error_code ec)
                {
                    if(ec || !m_advertising)
                        return;
                    emit_announcement();
                    arm_timer();
                });
    }

    std::uint64_t ttl_secs() const
    {
        return static_cast<std::uint64_t>(m_options.ttl);
    }

    executor_type m_executor;
    Socket &m_socket;
    typename Policy::timer_type m_timer;
    discovery_options m_options;
    detail::discovery_flood_cap<Clock> m_flood_cap;
    std::optional<wire::announcement> m_announcement;
    std::vector<std::byte> m_scratch;
    resolved_callback m_on_resolved_cb;
    ::plexus::discovery::discovery::withdrawn_callback m_on_withdrawn_cb;
    bool m_advertising;
};

}

#endif
