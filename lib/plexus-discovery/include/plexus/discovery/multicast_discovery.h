#ifndef HPP_GUARD_PLEXUS_DISCOVERY_MULTICAST_DISCOVERY_H
#define HPP_GUARD_PLEXUS_DISCOVERY_MULTICAST_DISCOVERY_H

#include "plexus/discovery/discovery_health.h"
#include "plexus/discovery/discovery_options.h"
#include "plexus/discovery/detail/announce_jitter.h"
#include "plexus/discovery/detail/announcement_card.h"
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
            , m_jitter(m_options.jitter_fraction)
            , m_bind_time(Clock::now())
            , m_universe_is_concrete(true)
            , m_advertising(false)
            , m_self_seen(false)
    {
        m_socket.bind(bind_endpoint());
        // Single source of truth: derive the concrete uint32 from the authoritative label so the
        // fast-path key and hard-scope group agree with the pattern. A consumer that left the default
        // label but set a custom uint32 (the legacy uint32-only shape) keeps it and stays legacy-concrete.
        if(m_options.universe_pattern != k_default_universe_label)
            m_options.universe = universe_from_label(m_options.universe_pattern);
        // Parse the local label once. A misconfigured label fails closed: m_universe_pattern stays empty
        // so the non-fast-path drops every flagged peer, while m_universe_is_concrete stays true so a
        // flagless legacy peer still takes the uint32 fast-path.
        if(auto pattern = match::key_pattern::make(m_options.universe_pattern))
        {
            m_universe_pattern     = *pattern;
            m_universe_is_concrete = match::detail::is_concrete(*pattern);
        }
        m_socket.on_datagram([this](const typename Socket::endpoint_type &from, std::span<const std::byte> bytes) { on_inbound(from, bytes); });
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
        emit_announcement();
        if(!was_advertising)
        {
            m_jitter.seed(detail::jitter_seed_from(m_announcement->node_id));
            arm_timer();
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
        // Fail-closed ahead of self-echo, goodbye, and admission: a foreign universe is dropped whole, evicting no peer and paying no admission or alloc.
        if(!universe_admits(*ann))
            return;
        // A node never notes itself: its own echo (same node_id) is recorded for the self-probe then dropped, keeping awareness cross-node.
        if(m_announcement && ann->node_id == m_announcement->node_id)
        {
            m_self_seen = true;
            return;
        }
        dispatch(*ann, from.address().to_string());
    }

    // The concrete/concrete common case keeps the legacy uint32 fast-path (no parse, no alloc);
    // otherwise the universe patterns must intersect — a rider on the shared matcher, never a second
    // glob. FNV(factory/line/*) != FNV(factory/line/1) yet they intersect, so a flagged peer must never
    // take the fast-path. A malformed peer pattern or an unparsable local pattern fails closed.
    bool universe_admits(const wire::announcement &ann) const
    {
        if(m_universe_is_concrete && !(ann.flags & wire::k_announcement_universe_pattern_flag))
            return ann.universe == m_options.universe;
        const auto peer = match::key_pattern::make(ann.universe_pattern);
        return m_universe_pattern && peer && m_universe_pattern->intersects(*peer);
    }

    void dispatch(const wire::announcement &ann, const std::string &source)
    {
        auto info = detail::service_info_from_announcement(ann, source);
        if((ann.flags & wire::k_announcement_goodbye_flag) != 0)
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
        // The universe-pattern presence flag is a persistent property of this node's announcement (set
        // at build from the label); goodbye is the only transient per-emit bit. Preserve the former so a
        // re-announce or goodbye never drops the pattern and silently reverts to legacy-flagless wire.
        m_announcement->flags = flags | (m_announcement->flags & wire::k_announcement_universe_pattern_flag);
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
        m_timer.expires_after(m_jitter.next(m_options.announce_period));
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
};

}

#endif
