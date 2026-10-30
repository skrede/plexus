#ifndef HPP_GUARD_PLEXUS_DISCOVERY_MULTICAST_DISCOVERY_H
#define HPP_GUARD_PLEXUS_DISCOVERY_MULTICAST_DISCOVERY_H

#include "plexus/discovery/discovery_health.h"
#include "plexus/discovery/discovery_options.h"
#include "plexus/discovery/detail/discovery_emit.h"
#include "plexus/discovery/detail/announce_jitter.h"
#include "plexus/discovery/detail/discovery_inbound.h"
#include "plexus/discovery/detail/discovery_flood_cap.h"
#include "plexus/discovery/detail/discovery_card_codec.h"

#include "plexus/match/key_pattern.h"
#include "plexus/match/detail/match_engine.h"

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
            , m_jitter(m_options.jitter_fraction)
            , m_bind_time(Clock::now())
            , m_universe_is_concrete(true)
            , m_advertising(false)
            , m_self_seen(false)
    {
        m_socket.bind(bind_endpoint());
        // Single source of truth: derive the concrete uint32 from the authoritative label so the
        // fast-path key and hard-scope group agree with the pattern. A consumer that left the default
        // label but set a custom uint32 (the concrete-only uint32 shape) keeps it and stays concrete.
        if(m_options.universe_pattern != k_default_universe_label)
            m_options.universe = universe_from_label(m_options.universe_pattern);
        // Parse the local label once. A misconfigured label fails closed: m_universe_pattern stays empty
        // so the non-fast-path drops every flagged peer, while m_universe_is_concrete stays true so a
        // flagless peer still takes the uint32 fast-path.
        if(auto pattern = match::key_pattern::make(m_options.universe_pattern))
        {
            m_universe_pattern     = *pattern;
            m_universe_is_concrete = match::detail::is_concrete(*pattern);
        }
        m_socket.on_datagram([this](const typename Socket::endpoint_type &from, std::span<const std::byte> bytes) { detail::on_inbound<Clock>(*this, from, bytes); });
    }

    // The self-probe verdict: healthy once the node has seen its own multicast echo, no_self_echo
    // once the window has elapsed without it (loopback disabled), else not_yet. bad_interface is a
    // resolve-time verdict the factory folds in, so it is never returned here.
    discovery_health probe(typename Clock::time_point now, std::chrono::milliseconds window) const
    {
        if(m_self_seen)
            return discovery_health::healthy;
        if(now - m_bind_time >= window)
            return discovery_health::no_self_echo;
        return discovery_health::not_yet;
    }

    void advertise(const ::plexus::discovery::service_info &service) override
    {
        m_announcement             = detail::announcement_from_service_info(service, ttl_secs(), m_options.universe, m_options.universe_pattern);
        const bool was_advertising = m_advertising;
        m_advertising              = true;
        detail::emit_announcement(*this);
        if(!was_advertising)
        {
            m_jitter.seed(detail::jitter_seed_from(m_announcement->node_id));
            detail::arm_timer(*this);
        }
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
            detail::emit_goodbye(*this);
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

    std::uint64_t ttl_secs() const
    {
        return static_cast<std::uint64_t>(m_options.ttl);
    }

    executor_type m_executor;
    Socket &m_socket;
    typename Policy::timer_type m_timer;
    discovery_options m_options;
    detail::discovery_flood_cap<Clock> m_flood_cap;
    detail::announce_jitter<std::chrono::milliseconds> m_jitter;
    typename Clock::time_point m_bind_time;
    std::optional<match::key_pattern> m_universe_pattern;
    bool m_universe_is_concrete;
    std::optional<wire::announcement> m_announcement;
    std::vector<std::byte> m_scratch;
    resolved_callback m_on_resolved_cb;
    ::plexus::discovery::discovery::withdrawn_callback m_on_withdrawn_cb;
    bool m_advertising;
    bool m_self_seen;

    template<typename C, typename D, typename Endpoint>
    friend void detail::on_inbound(D &, const Endpoint &, std::span<const std::byte>);
    template<typename D>
    friend bool detail::universe_admits(const D &, const wire::announcement &);
    template<typename C, typename D>
    friend void detail::dispatch_announcement(D &, const wire::announcement &, const std::string &);
    template<typename D>
    friend void detail::emit_with_flags(D &, std::uint8_t);
    template<typename D>
    friend void detail::arm_timer(D &);
};

}

#endif
