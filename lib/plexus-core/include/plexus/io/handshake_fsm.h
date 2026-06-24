#ifndef HPP_GUARD_PLEXUS_IO_HANDSHAKE_FSM_H
#define HPP_GUARD_PLEXUS_IO_HANDSHAKE_FSM_H

#include "plexus/io/handshake_protocol.h"
#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/attach_policy.h"
#include "plexus/io/host_fingerprint.h"

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
        if(auto gate = validate(req.protocol_version, req.id, req.fingerprint, facts))
            return *gate;
        if(!is_version_compatible(req.version_major, req.version_minor))
            return reject_version_result();
        return resolve_inbound(inbound_is_bootstrap);
    }

    fsm_step_result on_response(const wire::handshake_response &resp, const security::attach_facts &facts = {}) noexcept
    {
        m_last_seen_their_protocol_version = resp.protocol_version;
        if(auto gate = validate(resp.protocol_version, resp.id, resp.fingerprint, facts))
            return *gate;
        if(resp.status == wire::handshake_status::identity_conflict)
            return identity_conflict_result();
        if(resp.status != wire::handshake_status::accepted)
            return reject_version_result();
        if(!is_version_compatible(resp.version_major, resp.version_minor))
            return reject_version_result();
        return resolve_outbound();
    }

    fsm_step_result on_timeout() noexcept
    {
        if(m_state == peer_fsm_state::handshaking)
        {
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
        m_state            = peer_fsm_state::not_connected;
        m_inbound_pending  = false;
        m_complete_emitted = false;
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

    host_fingerprint local_fingerprint() const noexcept
    {
        return m_cfg.local_fingerprint;
    }

    const security::attach_policy *attach_policy() const noexcept
    {
        return m_cfg.attach_policy;
    }

private:
    std::optional<fsm_step_result> validate(std::uint8_t peer_protocol_version, const node_id &peer_id, std::uint64_t peer_fingerprint,
                                            const security::attach_facts &facts = {}) noexcept
    {
        m_peer_id          = peer_id;
        m_peer_fingerprint = host_fingerprint{peer_fingerprint};
        if(peer_protocol_version != wire::k_protocol_version)
            return reject_version_result();
        if(peer_id == m_cfg.self_id)
            return identity_conflict_result();
        if(m_cfg.attach_policy != nullptr && !m_cfg.attach_policy->decide(facts))
            return reject_unauthorized_result();
        return std::nullopt;
    }

    // A fresh inbound bootstrap completes inbound-only (no counter-dial to arbitrate). A mid-dial
    // inbound with an outbound in flight is the second arrival of a simultaneous connect: arbitrate.
    fsm_step_result resolve_inbound(bool inbound_is_bootstrap) noexcept
    {
        m_inbound_pending = true;
        if(racing_outbound())
            return m_complete_emitted ? accept_inbound_no_complete() : complete_inbound(arbitrate_dedup());
        if(inbound_is_bootstrap)
            return complete_inbound(dedup_decision::none);
        m_state = peer_fsm_state::handshake_resolved;
        return {.action = fsm_action::send_response, .outcome = handshake_outcome::accept_inbound};
    }

    fsm_step_result resolve_outbound() noexcept
    {
        if(m_state != peer_fsm_state::handshaking && m_state != peer_fsm_state::handshake_resolved)
            return {};
        if(m_complete_emitted)
            return {};
        auto dedup         = m_inbound_pending ? arbitrate_dedup() : dedup_decision::none;
        m_state            = peer_fsm_state::handshake_resolved;
        m_complete_emitted = true;
        return {.action = fsm_action::complete, .outcome = handshake_outcome::accept_outbound, .dedup = dedup};
    }

    bool racing_outbound() const noexcept
    {
        return m_state == peer_fsm_state::handshaking || m_state == peer_fsm_state::handshake_resolved;
    }

    fsm_step_result complete_inbound(dedup_decision dedup) noexcept
    {
        m_state            = peer_fsm_state::handshake_resolved;
        m_complete_emitted = true;
        return {.action = fsm_action::complete, .outcome = handshake_outcome::accept_inbound, .dedup = dedup};
    }

    static fsm_step_result accept_inbound_no_complete() noexcept
    {
        return {.action = fsm_action::send_response, .outcome = handshake_outcome::accept_inbound};
    }

    // Greater node_id keeps its outbound; the loser keeps the inbound it accepted. The equal case is
    // rejected at validation, so the comparison is strict and both sides agree on the survivor.
    dedup_decision arbitrate_dedup() const noexcept
    {
        return m_cfg.self_id > m_peer_id ? dedup_decision::keep_outbound : dedup_decision::keep_inbound;
    }

    bool is_version_compatible(std::uint8_t peer_major, std::uint8_t peer_minor) const noexcept
    {
        return (peer_major > m_cfg.compatible_version_major) || (peer_major == m_cfg.compatible_version_major && peer_minor >= m_cfg.compatible_version_minor);
    }

    fsm_step_result abort_result(handshake_outcome reason) noexcept
    {
        m_state = peer_fsm_state::not_connected;
        return {.action = fsm_action::abort, .outcome = reason};
    }

    fsm_step_result reject_version_result() noexcept
    {
        return abort_result(handshake_outcome::reject_version);
    }

    fsm_step_result identity_conflict_result() noexcept
    {
        return abort_result(handshake_outcome::reject_identity);
    }

    fsm_step_result reject_unauthorized_result() noexcept
    {
        return abort_result(handshake_outcome::reject_unauthorized);
    }

    node_id m_peer_id;
    bool m_inbound_pending;
    peer_fsm_state m_state;
    bool m_complete_emitted;
    handshake_fsm_config m_cfg;
    host_fingerprint m_peer_fingerprint;
    std::uint8_t m_last_seen_their_protocol_version;
};

}

#endif
