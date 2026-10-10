#ifndef HPP_GUARD_PLEXUS_IO_HANDSHAKE_FSM_H
#define HPP_GUARD_PLEXUS_IO_HANDSHAKE_FSM_H

#include "plexus/io/host_fingerprint.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/io/detail/handshake_resolve.h"

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/attach_policy.h"

#include "plexus/wire/handshake.h"

#include "plexus/node_id.h"

#include <cstdint>
#include <optional>

namespace plexus::io {

// Pure, sans-IO handshake state machine: holds no asio / transport / logger types and moves no
// bytes. It does NOT log — it returns abort/reject and the bridge logs at action-execution time.
class handshake_fsm
{
public:
    explicit handshake_fsm(const handshake_fsm_config &cfg) noexcept
            : m_peer_id{}
            , m_inbound_pending{false}
            , m_state{peer_fsm_state::not_connected}
            , m_complete_emitted{false}
            , m_cfg(cfg)
            , m_peer_fingerprint{}
            , m_last_seen_their_protocol_version{0}
            , m_handshake_retry_remaining{cfg.handshake_retry}
    {
    }

    fsm_step_result on_dial_started() noexcept
    {
        if(m_state == peer_fsm_state::not_connected || m_state == peer_fsm_state::dialing)
            m_state = peer_fsm_state::dialing;
        return {};
    }

    fsm_step_result on_outbound_connected() noexcept
    {
        if(m_state == peer_fsm_state::handshaking || m_state == peer_fsm_state::handshake_resolved)
            return {};
        m_state = peer_fsm_state::handshaking;
        return {.action = fsm_action::send_request};
    }

    // inbound_is_bootstrap marks a fresh inbound with no counter-direction dial in flight (the
    // common path under demand-driven lazy dial): it completes inbound-only rather than stranding.
    fsm_step_result on_request(const wire::handshake_request &req, bool inbound_is_bootstrap, const security::attach_facts &facts = {}) noexcept
    {
        m_last_seen_their_protocol_version = req.protocol_version;
        if(auto gate = detail::validate(*this, req.protocol_version, req.id, req.fingerprint, facts))
            return *gate;
        if(!detail::is_version_compatible(*this, req.version_major, req.version_minor))
            return detail::reject_version_result(*this);
        return detail::resolve_inbound(*this, inbound_is_bootstrap);
    }

    fsm_step_result on_response(const wire::handshake_response &resp, const security::attach_facts &facts = {}) noexcept
    {
        m_last_seen_their_protocol_version = resp.protocol_version;
        if(auto gate = detail::validate(*this, resp.protocol_version, resp.id, resp.fingerprint, facts))
            return *gate;
        if(resp.status == wire::handshake_status::identity_conflict)
            return detail::identity_conflict_result(*this);
        if(resp.status != wire::handshake_status::accepted)
            return detail::reject_version_result(*this);
        if(!detail::is_version_compatible(*this, resp.version_major, resp.version_minor))
            return detail::reject_version_result(*this);
        return detail::resolve_outbound(*this);
    }

    fsm_step_result on_timeout() noexcept
    {
        if(m_state == peer_fsm_state::handshaking)
        {
            if(m_handshake_retry_remaining > 0)
            {
                --m_handshake_retry_remaining;
                return {.action = fsm_action::send_request};
            }
            m_state = peer_fsm_state::not_connected;
            return {.action = fsm_action::abort};
        }
        if(m_state == peer_fsm_state::dialing)
            return {.action = fsm_action::retry};
        return {};
    }

    // A crypto-handshake transport completed its own mutual-cert handshake, so the session resolves
    // with no plexus wire round-trip. Latched once via m_complete_emitted; on_torn_down resets it.
    fsm_step_result on_external_complete() noexcept
    {
        if(m_complete_emitted)
            return {};
        m_state            = peer_fsm_state::handshake_resolved;
        m_complete_emitted = true;
        return {.action = fsm_action::complete, .outcome = handshake_outcome::accept_outbound};
    }

    fsm_step_result on_torn_down() noexcept
    {
        m_state                     = peer_fsm_state::not_connected;
        m_inbound_pending           = false;
        m_complete_emitted          = false;
        m_handshake_retry_remaining = m_cfg.handshake_retry;
        return {};
    }

    peer_fsm_state state() const noexcept
    {
        return m_state;
    }

    std::uint8_t last_seen_their_protocol_version() const noexcept
    {
        return m_last_seen_their_protocol_version;
    }

    host_fingerprint last_seen_peer_fingerprint() const noexcept
    {
        return m_peer_fingerprint;
    }

    // The identity the peer proved in the handshake, which an accepted session learns only there:
    // its slot is keyed by a provisional id minted before anyone spoke.
    const node_id &last_seen_peer_id() const noexcept
    {
        return m_peer_id;
    }

    host_fingerprint local_fingerprint() const noexcept
    {
        return m_cfg.local_fingerprint;
    }

    const security::attach_policy *attach_policy() const noexcept
    {
        return m_cfg.attach_policy;
    }

private:
    node_id m_peer_id;
    bool m_inbound_pending;
    peer_fsm_state m_state;
    bool m_complete_emitted;
    handshake_fsm_config m_cfg;
    host_fingerprint m_peer_fingerprint;
    std::uint8_t m_last_seen_their_protocol_version;
    std::uint32_t m_handshake_retry_remaining;

    template<typename F>
    friend fsm_step_result detail::abort_result(F &, handshake_outcome) noexcept;
    template<typename F>
    friend std::optional<fsm_step_result> detail::validate(F &, std::uint8_t, const node_id &, std::uint64_t, const security::attach_facts &) noexcept;
    template<typename F>
    friend bool detail::racing_outbound(const F &) noexcept;
    template<typename F>
    friend fsm_step_result detail::complete_inbound(F &, dedup_decision) noexcept;
    template<typename F>
    friend dedup_decision detail::arbitrate_dedup(const F &) noexcept;
    template<typename F>
    friend fsm_step_result detail::resolve_inbound(F &, bool) noexcept;
    template<typename F>
    friend fsm_step_result detail::resolve_outbound(F &) noexcept;
    template<typename F>
    friend bool detail::is_version_compatible(const F &, std::uint8_t, std::uint8_t) noexcept;
};

}

#endif
